# Hướng dẫn triển khai mục 4.4 — Mage pipelines

**Owner:** Project Lead/Integration  
**Ngày triển khai:** 2026-11-17 (Asia/Bangkok)  
**Trạng thái:** CODE VÀ TEST 4.4 HOÀN TẤT; integration ghi Physical Staging chờ DDL 4.5  
**Phụ thuộc:** 4.1–4.3 hoàn tất; 4.5 phải tạo năm bảng vật lý trước khi chạy exporter.

## 1. Mục tiêu và ranh giới trách nhiệm

Mục 4.4 triển khai hai pipeline Mage:

1. `pl_reference_taxi_zone`: đọc và kiểm tra Taxi Zone lookup cùng Shapefile.
2. `pl_yellow_taxi_ingestion`: discovery, checksum/schema validation, đọc Parquet theo row group, canonicalization kỹ thuật, technical quarantine và ghi Physical Staging.

Mage chỉ chịu trách nhiệm technical ingestion. Mage không phân loại business quarantine, không tính KPI, không xóa business duplicate và không map surrogate dimension keys. Các việc đó thuộc dbt từ checkpoint 4.6.

Ranh giới bắt buộc:

```text
Mage
  stage.yellow_taxi_raw
  stage.yellow_taxi_canonical
  dq.technical_quarantine
        |
        v
dbt
  business DQ
  duplicate classification
  dimensions/fact/KPI eligibility
```

## 2. Quyết định đường dẫn

Mage chạy trong Docker nên code không được dùng `D:\BI\DATH` hoặc đường dẫn Windows khác.

| Host Windows | Mage container |
|---|---|
| `D:\BI\DATH\NYC_TAXI_BI_PROJECT\data` | `/home/src/data` |
| `D:\BI\DATH\NYC_TAXI_BI_PROJECT\mage_project` | `/home/src/mage_project/nyc_taxi_bi` |
| `data/reference/taxi+_zone_lookup.csv` | `/home/src/data/reference/taxi+_zone_lookup.csv` |
| `data/reference/taxi_zones/taxi_zones.shp` | `/home/src/data/reference/taxi_zones/taxi_zones.shp` |
| `data/catalog/yellow_taxi_2024_2025.csv` | `/home/src/data/catalog/yellow_taxi_2024_2025.csv` |

Catalog tiếp tục lưu đường dẫn portable dạng `data/yellow_taxi/...`. Hàm `utils.paths.resolve_data_path` bỏ prefix `data/`, nối vào `/home/src/data` và chặn path traversal.

Không cần sửa `docker-compose.yml`; mount hiện tại đã đúng:

```yaml
volumes:
  - ./mage_project:/home/src/mage_project/${MAGE_PROJECT_NAME}
  - ./data:/home/src/data
```

Work files được tạo tại:

```text
/home/src/data/work/task_4_4/<batch_id>/
├── raw/
├── canonical/
└── technical_reject/
```

`data/work/**` đã được thêm vào `.gitignore` vì đây là runtime artifact, không phải source code.

## 3. File implementation

```text
mage_project/
├── utils/
│   ├── __init__.py
│   ├── paths.py
│   ├── database.py
│   ├── parquet_utils.py
│   ├── canonical.py
│   └── stage_loader.py
├── data_loaders/
│   ├── load_zone_lookup.py
│   ├── load_zone_geometry.py
│   └── get_extraction_window.py
├── transformers/
│   ├── validate_zone_lookup.py
│   ├── validate_zone_geometry.py
│   ├── discover_source_files.py
│   ├── verify_checksum.py
│   ├── filter_incremental.py
│   ├── load_source_files.py
│   ├── canonicalize_schema.py
│   ├── standardize_and_validate.py
│   └── verify_stage.py
├── data_exporters/
│   ├── load_reference_stage.py
│   ├── write_technical_quarantine.py
│   └── load_stage.py
├── pipelines/
│   ├── pl_reference_taxi_zone/metadata.yaml
│   └── pl_yellow_taxi_ingestion/metadata.yaml
└── tests/
    ├── unit/test_task_4_4_utilities.py
    └── integration/test_task_4_4_project_inputs.py

tests/
└── run_phase2_4_4_tests.py
```

## 4. Shared utilities

### `paths.py`

- Chuyển catalog path thành container path.
- Không hard-code máy cá nhân.
- Gọi `Path.relative_to` để ngăn `../` thoát khỏi mounted data root.

Ví dụ:

```text
data/yellow_taxi/by_year_month/2024-05/file.parquet
-> /home/src/data/yellow_taxi/by_year_month/2024-05/file.parquet
```

### `database.py`

- Đọc connection properties từ environment.
- Không ghi password/connection string vào log.
- Cung cấp `fetch_one`, `fetch_all`, `execute`.
- `require_tables` fail sớm và nêu đúng bảng 4.5 còn thiếu.

### `parquet_utils.py`

- SHA-256 được tính streaming theo chunk 8 MB.
- Footer Parquet phải đọc được và row count phải dương.
- Required columns: pickup, dropoff, PU, DO và total.
- Schema fingerprint dùng ordered `(ordinal, name, logical_type)` và JSON serialization compact giống script tạo catalog.
- `source_record_key = SHA256(lower(file_sha256) || ':' || row_number)`.

### `canonical.py`

- Normalize tên cột, gồm `Airport_fee` thành `airport_fee`.
- Tạo `cbd_congestion_fee = null` khi structurally absent ở schema 2024.
- Try-cast timestamp, integer và numeric.
- Chỉ technical reject khi required field null/uncastable.
- Amount âm, distance âm và store flag lạ không phải technical reject.
- Giữ `raw_payload` và `cast_error_flags`.

### `stage_loader.py`

- Chỉ insert phần giao giữa DataFrame columns và target table columns.
- Bắt buộc target có `source_record_key`.
- Ghi qua temporary table rồi `ON CONFLICT (source_record_key) DO NOTHING`.
- JSON được cast rõ sang `jsonb`.
- Không dùng `if_exists='replace'`, do đó không làm mất constraint/index của 4.5.

## 5. Pipeline reference

```text
load_zone_lookup --------> validate_zone_lookup ----\
                                                   > validate_zone_geometry
load_zone_geometry -------------------------------/           |
                                                              v
                                                   load_reference_stage
```

### `load_zone_lookup`

Đọc `/home/src/data/reference/taxi+_zone_lookup.csv` với `LocationID` là nullable integer và ba field text.

### `validate_zone_lookup`

Success criteria:

- 265 rows.
- 265 LocationID unique và non-null.
- Đủ tập ID 1–265.
- Borough và zone name có giá trị.
- Zone 264/265 giữ nguyên là business source members.

### `load_zone_geometry`

Trước khi đọc, block bắt buộc tìm thấy `.shp`, `.shx`, `.dbf`, `.prj`. GeoPandas dùng engine `pyogrio`.

### `validate_zone_geometry`

Success criteria:

- 263 features.
- Source CRS là EPSG:2263.
- LocationID unique/non-null.
- Geometry non-null, non-empty và valid.
- Chỉ Polygon/MultiPolygon.
- Chuẩn hóa Polygon thành MultiPolygon.
- Lookup IDs thiếu geometry đúng bằng `{264, 265}`.

### `load_reference_stage`

Exporter yêu cầu:

```text
stage.taxi_zone_raw
stage.taxi_zone_geometry
```

Hai bảng được kiểm tra trước khi transaction bắt đầu. Lookup và geometry được reload trong cùng transaction; geometry lưu `MULTIPOLYGON` SRID 2263.

## 6. Pipeline Yellow Taxi

```text
get_extraction_window
  -> discover_source_files
  -> verify_checksum
  -> filter_incremental
  -> load_source_files
  -> canonicalize_schema
  -> standardize_and_validate
  -> write_technical_quarantine
  -> load_stage
  -> verify_stage
```

### 6.1 `get_extraction_window`

Gọi `control.start_batch` với grain:

```text
pipeline_name = pl_taxi_master
source_name   = yellow_taxi_tripdata
```

Block nhận `BACKFILL`, `INCREMENTAL`, `RETRY`, `TEST`; RETRY bắt buộc có `parent_batch_id`. CET chỉ lấy từ PostgreSQL một lần và cùng `batch_id` được truyền đến cuối pipeline.

### 6.2 `discover_source_files`

Runtime variables:

```yaml
run_type: BACKFILL
source_year_month_from: '2024-01'
source_year_month_to: '2025-12'
catalog_path: /home/src/data/catalog/yellow_taxi_2024_2025.csv
schema_registry_path: /home/src/data/catalog/yellow_taxi_schema_registry.csv
work_root: /home/src/data/work/task_4_4
```

Block thực hiện:

1. Filter catalog theo scope tháng.
2. `control.start_step(..., 'DISCOVERY')`.
3. `should_discover_file` cho từng catalog row.
4. `discover_file` cho file được chọn.
5. `update_discovery_result` với expected/checked bằng số file trong scope.
6. Finish DISCOVERY SUCCESS hoặc fail batch.

Full scope phải có 24/24; subset test dùng số file thực tế trong subset.

### 6.3 `verify_checksum`

Với mỗi selected file:

1. Resolve container path.
2. Chuyển lifecycle sang DOWNLOADING.
3. Tính SHA-256 streaming và so catalog.
4. Đọc footer/schema.
5. Tra schema fingerprint trong registry.
6. Gọi `control.validate_file`.

Action handling:

| Action | Xử lý |
|---|---|
| `PROCESS` | Tiếp tục |
| `REPROCESS_IDEMPOTENTLY` | Tiếp tục; staging dùng source key |
| `SKIP_ALREADY_PROCESSED` | Không đọc row data |
| `STOP_SOURCE_VERSION_CHANGED` | Fail batch và yêu cầu approval |

### 6.4 `filter_incremental`

Chỉ giữ `PROCESS` và `REPROCESS_IDEMPOTENTLY`. Watermark lọc theo `source_last_modified`, không theo pickup datetime.

### 6.5 `load_source_files`

- Đọc từng row group, không concat 24 file vào RAM.
- `source_row_number` chạy liên tục từ 1 trong mỗi file.
- Tạo stable `source_record_key`.
- Thêm batch/file/schema lineage.
- Ghi bounded raw work Parquet rồi giải phóng DataFrame trước chunk tiếp theo.

### 6.6 `canonicalize_schema`

- Đọc từng raw work chunk.
- Canonicalize technical fields.
- Tách accepted canonical và technical reject.
- Serialize `raw_payload`/`cast_error_flags` thành JSON text trước khi ghi Parquet; việc này tránh lỗi Arrow khi dictionary rỗng.

### 6.7 `standardize_and_validate`

Enforce tuyệt đối:

```text
rows_raw = rows_canonical + rows_technical_quarantined
```

Business anomaly vẫn ở canonical để dbt xử lý.

### 6.8 `write_technical_quarantine`

Yêu cầu `dq.technical_quarantine`. Mỗi chunk được ghi idempotently bằng `source_record_key`.

### 6.9 `load_stage`

Yêu cầu:

```text
stage.yellow_taxi_raw
stage.yellow_taxi_canonical
```

Block start step STAGING, upsert từng chunk, rồi gọi `update_file_result(..., 'STAGED', ...)` theo file. Nếu lỗi, block finish STAGING FAILED và gọi `fail_batch`; watermark không đổi.

### 6.10 `verify_stage`

- Đếm theo file SHA, vì retry có thể giữ batch ID cũ trên row đã tồn tại.
- Kiểm `raw = canonical + technical quarantine`.
- Finish STAGING SUCCESS.
- Ghi counters ban đầu.
- Chuyển batch `RUNNING -> STAGED`.

Block không gọi `commit_batch_success`: dbt, business DQ và reconciliation chưa chạy.

## 7. Test suite

Chạy trong Mage container để dùng đúng pandas/PyArrow/GeoPandas versions:

```powershell
docker compose up -d postgres mage
docker compose exec -T mage bash -lc "cd /home/src/mage_project && python -m unittest discover -s nyc_taxi_bi/tests -t . -p 'test_task_4_4*.py' -v"
```

Các test hiện có:

1. Catalog path mapping.
2. Path traversal rejection.
3. SHA-256 streaming.
4. Schema fingerprint serialization.
5. Stable/position-sensitive source key.
6. Valid canonical row và invalid timestamp reject.
7. Business anomaly không bị technical reject.
8. Empty error JSON ghi được Parquet.
9. Catalog đủ 24 validated files trên disk.
10. File 2024/2025 map đúng schema v001/v002.
11. Lookup/geometry contract 265/263, EPSG:2263, missing `{264,265}`.

Kết quả ngày 2026-11-17:

```text
Ran 11 tests in 0.289s
OK
```

Pipeline metadata validation:

```text
pl_reference_taxi_zone: 5 blocks
pl_yellow_taxi_ingestion: 10 blocks
```

Mage restart không phát hiện startup error.

## 8. Trạng thái dependency 4.5

Truy vấn `to_regclass` ngày 2026-11-17 xác nhận năm bảng sau chưa tồn tại:

```text
stage.yellow_taxi_raw
stage.yellow_taxi_canonical
stage.taxi_zone_raw
stage.taxi_zone_geometry
dq.technical_quarantine
```

Do đó:

- Code, DAG, path handling, checksum/schema logic và test 4.4 đã hoàn tất.
- Không chạy exporter thật trước 4.5.
- `require_tables` sẽ fail có chủ đích với danh sách bảng thiếu.
- Không tạo bảng tự động bằng pandas `replace` vì sẽ phá physical contract 4.5.

## 9. Trình tự chạy sau khi hoàn tất 4.5

1. Chạy DDL 4.5.
2. Kiểm năm bảng bằng `to_regclass`.
3. Chạy `pl_reference_taxi_zone`.
4. Kiểm 265 lookup rows, 263 geometry rows, SRID 2263.
5. Chạy Yellow Taxi scope một tháng 2024-05.
6. Chạy scope 2025-05 để kiểm schema v002/CBD fee.
7. Chạy invalid fixture/technical reject.
8. Test retry và xác nhận không duplicate source key.
9. Chạy full BACKFILL 2024-01..2025-12.
10. Thu evidence per-file row counts và chuyển checkpoint 4.6.

## 10. Definition of Done 4.4

- [x] Hai Mage pipeline và metadata đã tạo.
- [x] Host/container paths portable, không hard-code máy cá nhân.
- [x] Reference CSV/Shapefile validation code hoàn tất.
- [x] LocationID, CRS, geometry và expected 264/265 gap được test.
- [x] Control LSET/CET/batch contract được dùng.
- [x] Discovery/checksum/schema registry lifecycle được triển khai.
- [x] Đọc Parquet theo row group.
- [x] Lineage và stable source record key được tạo.
- [x] Technical-only canonicalization và quarantine split được test.
- [x] Stage/quarantine writer có idempotency và table preflight.
- [x] 11 unit/integration tests pass trong Mage container.
- [ ] Reference exporter chạy thật — chờ bảng 4.5.
- [ ] Yellow staging/reconciliation chạy thật — chờ bảng 4.5.
- [ ] Full 24-file BACKFILL evidence — thực hiện sau 4.5.

Không đánh dấu ba mục cuối trước khi Physical Staging tồn tại và integration run thành công.
