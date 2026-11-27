# Runbook Mage pipelines — kiến trúc hiện tại

Ngày cập nhật: 2026-11-27. Áp dụng cho Docker Compose của project và staging contract `slim-v2`.

## 1. Bốn pipeline hiển thị trong Mage

| Pipeline | Chức năng | Khi chạy |
|---|---|---|
| `pl_database_connection_test` | Kiểm tra kết nối Mage → PostgreSQL | Khi setup hoặc chẩn đoán kết nối |
| `pl_control_smoke_validation` | Kiểm control batch và truyền `batch_id` giữa block | Smoke test control, không nạp trip |
| `pl_reference_taxi_zone` | Nạp/kiểm 265 zone lookup và 263 geometry EPSG:2263 | Lần đầu hoặc khi reference thay đổi |
| `pl_yellow_taxi_ingestion` | Discovery → checksum/schema → canonical/DQ → STAGED | Backfill/incremental Yellow Taxi |

Các thư mục pipeline có `.gitkeep` nhưng không có `metadata.yaml` chỉ là placeholder, không phải pipeline runtime.

## 2. Khởi động

Từ `D:\BI\DATH\NYC_TAXI_BI_PROJECT`:

```powershell
docker compose up -d
docker compose ps
```

Yêu cầu PostgreSQL `healthy`; Mage mở tại `http://localhost:6789`.

Không dùng `docker compose down -v` để reset vì lệnh đó xóa PostgreSQL volume, reference và migration.

## 3. Kiểm tra contract live

```powershell
docker compose exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT storage_mode FROM stage.stage_storage_contract;"'
```

Kết quả hiện tại phải là:

```text
slim-v2
```

Biến Mage vẫn dùng `stage_storage_mode: slim`, không nhập `slim-v2` vào UI.

## 4. Nạp reference

Chạy `pl_reference_taxi_zone` khi database mới hoặc reference thay đổi. Kết quả chấp nhận:

- `stage.taxi_zone_raw`: 265 dòng;
- `stage.taxi_zone_geometry`: 263 dòng;
- geometry SRID 2263;
- location 264/265 không có polygon theo nguồn.

Không cần chạy lại reference trước mỗi tháng trip.

## 5. Cấu hình Yellow Taxi ingestion

Ví dụ một tháng:

```yaml
prepare_mode: streaming
stage_load_mode: direct
stage_storage_mode: slim
source_batch_size: 50000
canonical_batch_size: 50000
catalog_path: /home/src/data/catalog/yellow_taxi_selected_2_months.csv
run_type: BACKFILL
source_year_month_from: 2024-05
source_year_month_to: 2024-05
```

`from` và `to` giống nhau để xử lý đúng một file/version. Catalog có thể chứa nhiều tháng; extraction window mới quyết định tháng được chọn.

## 6. DAG Yellow Taxi

```text
get_extraction_window
  → discover_source_files
  → verify_checksum
  → filter_incremental
  → load_source_files
  → canonicalize_schema
  → standardize_and_validate
  → write_technical_quarantine
  → load_stage
  → verify_stage
```

Trong streaming mode, ba block prepare chỉ chuyển manifest JSON. Việc đọc Parquet, canonicalize và COPY bounded chunks diễn ra trong `load_stage`.

## 7. `load_stage` slim-v2

```text
Parquet chunks
  → unified Raw + Canonical projection
  → one continuous COPY: stage.yellow_taxi_trip_data
  → INSERT technical rejects server-side
  → validate count/key/scope/partition
  → rebuild PK/unique
  → commit receipt + STAGED
```

Hai relation bàn giao cho dbt vẫn là:

- `stage.yellow_taxi_raw` — toàn bộ source rows;
- `stage.yellow_taxi_canonical` — accepted technical rows.

Không chạy riêng block `load_stage` với output upstream cũ. Chạy toàn pipeline để context batch/file/checksum nhất quán.

## 8. Kết quả và giới hạn

Pipeline thành công kết thúc ở `STAGED`. Nó chưa chạy:

- dbt/DDS;
- business/duplicate DQ;
- reconciliation cuối;
- commit watermark;
- cleanup staging;
- OLAP/Power BI.

Do đó batch STAGED sẽ chặn tháng tiếp theo. Luồng production phải hoàn thành dbt/DDS + tests + reconciliation + watermark + cleanup trước khi nạp file mới.

## 9. Reset chỉ dành cho benchmark Mage

Chỉ dùng khi không chạy dbt/DDS và muốn đo tháng khác. Ghi lại evidence trước khi reset.

```powershell
$resetSql = @'
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('yellow_taxi_backfill_runner'));
TRUNCATE TABLE
  stage.yellow_taxi_trip_data,
  stage.stage_file_batch,
  dq.technical_quarantine,
  audit.trip_fingerprint_history,
  audit.cross_file_fingerprint_match,
  control.file_reconciliation_history,
  control.etl_replacement,
  control.etl_backfill_file,
  control.etl_backfill_run,
  control.etl_log,
  control.etl_step_run,
  control.etl_file,
  control.source_file_version,
  control.etl_batch,
  control.etl_watermark
RESTART IDENTITY CASCADE;
INSERT INTO control.etl_watermark(
  pipeline_name,source_name,last_successful_extraction_time,
  last_successful_batch_id,watermark_version
) VALUES (
  'pl_taxi_master','yellow_taxi_tripdata',
  '1970-01-01 00:00:00+00',NULL,0
);
COMMIT;
CHECKPOINT;
'@

$resetSql | docker compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
if ($LASTEXITCODE -ne 0) { throw 'Reset benchmark thất bại' }
```

Kiểm tra:

```powershell
docker compose exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT (SELECT count(*) FROM stage.yellow_taxi_trip_data)||''|''||(SELECT count(*) FROM stage.stage_file_batch)||''|''||(SELECT count(*) FROM control.etl_batch)||''|''||(SELECT count(*) FROM dq.technical_quarantine);"'
```

Kết quả sạch là `0|0|0|0`. Reset không xóa source Parquet, reference hoặc `slim-v2`.

## 10. Benchmark đã xác nhận

QA full-month:

| Tháng | Dòng | `load_stage` | Source → STAGED |
|---|---:|---:|---:|
| 2024-05 | 3.723.833 | 107,110 giây | 110,551 giây |
| 2025-05 | 4.591.845 | 125,800 giây | 131,577 giây |

Người dùng đã chạy UI 2024-05 khoảng 2 phút và chạy thành công 2025-05. Chưa có số đo Source → DDS; không dùng các số trên để tuyên bố toàn ETL dưới 3 phút.

## 11. Xử lý lỗi thường gặp

- `Active or STALE batch already exists`: batch trước còn STAGED/RUNNING; hoàn tất downstream hoặc reset benchmark.
- `Installed staging contract requires matching slim mode`: giữ `stage_storage_mode=slim`; kiểm marker database.
- Dirty target: không xóa tay một phần; dùng recovery/runbook hoặc reset benchmark toàn scope.
- Lỗi sau COPY: transaction phải rollback; kiểm batch/step và actual counts, không ép SUCCESS.
- Mage dùng utility cache cũ: restart `mage` khi staging sạch, không rebuild/xóa volume.

## 12. Bằng chứng cần lưu

- tháng, checksum và row count nguồn;
- thời gian từng block và tổng pipeline;
- rows Raw/Canonical/Technical;
- trạng thái batch/file/step;
- dbt/tests/reconciliation nếu đo end-to-end;
- dung lượng trước/sau cleanup;
- cấu hình variables và commit Git.
