# dbt DDS — NYC Yellow Taxi

dbt nhận dữ liệu đã chuyển kiểu từ Mage, chuẩn hóa giá trị, tính các trường phân tích và phân loại chuyến vào Fact hoặc quarantine. Runner bật `dbt_minimal_mode: true`; mặc định của lời gọi dbt thủ công vẫn là `false` cho đường legacy. Không trộn hai mode trên cùng database đang có dữ liệu.

## Luồng chính

```text
Catalog/checksum/schema registry
  → Mage: yellow_taxi_raw + yellow_taxi_canonical + technical quarantine
  → staging → enriched → fingerprinted → classified
  → 6 Dimensions + Fact + business/duplicate quarantine
  → file_reconciliation + 15 batch tests
  → commit SUCCESS/watermark
  → cleanup raw/canonical/classification
```

[run_24_month_backfill.py](../deployment/scripts/run_24_month_backfill.py) điều phối toàn bộ lượt nạp, gọi trực tiếp các hàm block Mage rồi dbt. Cùng checksum/version đã SUCCESS sẽ skip trước dbt; cleanup pending được xử lý riêng. Reporting/OLAP không là gate commit DDS, chạy bằng job riêng sau nghiệm thu DDS. T07 và lịch live chưa được nghiệm thu.

## Chuẩn bị

1. Chọn database đích đã có PostgreSQL/PostGIS, control/staging DDL và migration phù hợp. Database hiện hữu cần [migration guide](../docs/etl_design/DBT_MINIMAL_T09_MIGRATION.md); không dùng full-refresh Fact để migrate.
2. Có Parquet và catalog đã VALIDATED: `data/catalog/yellow_taxi_2024_2025.csv`, schema registry `yellow_taxi_schema_registry.csv`. Zone lookup/geometry phải được nạp bằng luồng reference Mage trước khi nạp trips. Source contract tại [taxi_sources.yml](models/sources/taxi_sources.yml).
3. Cấu hình `.env` theo `.env.example` và profile từ [profiles.yml.example](profiles.yml.example). Đặt `POSTGRES_HOST`, `POSTGRES_HOST_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `DBT_PROFILES_DIR` đúng target. Controller chạy trên host dùng địa chỉ/port host, không lấy hostname nội bộ Docker làm địa chỉ host.
4. Role dbt cần quyền tạo/ghi DDS/DQ và `ANALYZE` raw/canonical/seed/workspace. PostgreSQL16 yêu cầu ownership/role chủ sở hữu phù hợp; kiểm warning thiếu quyền ANALYZE.
5. Có dự báo peak disk dương qua `--expected-peak-bytes`; controller chính giữ kiểm dự phòng25% filesystem của repo. Ngoại lệ disk25GiB/cap32GiB của benchmark QA cũ không áp dụng vào runner chính. Phải kiểm thêm ổ chứa PostgreSQL/Docker nếu khác ổ repo.

Mage mới từ `origin/main` dùng slim-v2: `stage.yellow_taxi_trip_data` là bảng vật lý, Raw/Canonical là view. Database đích cần migrations Mage theo tên đầy đủ: `008_mage_raw_cast_payload.sql`, `009_slim_stage_optin.sql`, `010_slim_v2_unified_stage.sql`, cùng DDS `008_minimal_dds_metadata.sql`. Các migration có gate staging rỗng/batch inactive, không được tự áp lên live. Xem [Mage stage contract](../docs/etl_design/MAGE_STAGE_CONTRACT.md). Runner hỗ trợ wide, slim-v1 và slim-v2; phải chọn storage đúng database.

## Chạy một tháng qua luồng chính

Chạy từ root repo sau khi cấu hình đúng target, có nguồn/reference và peak forecast thực tế. Ví dụ dùng một tháng cố định để kiểm tra; đổi tháng theo catalog.

```powershell
# Gán từ dự báo peak bytes của database/máy đích, không dùng một số mẫu để vượt gate.
if (!$env:DDS_EXPECTED_PEAK_BYTES) { throw 'Thiếu dự báo DDS_EXPECTED_PEAK_BYTES' }
$env:ALLOW_STAGING_CLEANUP = '1'
$env:MAGE_STAGE_LOAD_MODE = 'direct'
$env:MAGE_PREPARE_MODE = 'streaming'
$env:MAGE_STAGE_STORAGE = 'slim'
$env:MAGE_BATCH_SIZE = '50000'

& ./.venv-dbt-312/Scripts/python.exe deployment/scripts/run_24_month_backfill.py `
    --run-type INCREMENTAL `
    --from-month 2024-05 `
    --to-month 2024-05 `
    --enable-cleanup `
    --expected-peak-bytes $env:DDS_EXPECTED_PEAK_BYTES
```

Lượt `INCREMENTAL` hoàn tất parent sau batch gates. Nếu file cùng checksum đã SUCCESS, lệnh trên kiểm chứng skip, không phải phép đo nạp mới. Để đo initial load, dùng database QA mới đã chuẩn bị đúng contract; không xóa trạng thái SUCCESS/watermark hoặc ép replay để gọi đó là initial load.

`run_dds_incremental.py` chọn tháng VALIDATED mới nhất từ catalog cho scheduler, dùng `DDS_EXPECTED_PEAK_BYTES` và tùy chọn `DDS_CATALOG_PATH`. Chỉ dùng một scheduler/writer. Cấu hình scheduler live là bước triển khai riêng.

`--run-type BACKFILL` dùng cho danh sách nhiều file và cần cleanup theo file. Parent backfill chờ full audit/validation; không gọi finalize để bỏ qua gate. Nạp đủ24 file còn cần approval riêng của controller.

## dbt selectors và cấu hình

Runner gọi lần lượt seed khi thiếu → `dbt run --selector minimal_dds_run` → `dbt test --selector minimal_dds_batch_tests` → reconciliation → commit. Hai biến scope bắt buộc: `load_batch_id`, `load_file_version_id` đúng attempt writable. Không gọi lại materialization với batch SUCCESS hoặc dùng `dbt build` toàn project thay luồng này.

- [dbt_project.yml](dbt_project.yml): ngưỡng DQ, bands, mapping/schema, relation technical quarantine. Không lọc pickup chỉ vì nằm ngoài năm team chọn.
- [dq_rule_policy.csv](seeds/dq_rule_policy.csv): severity/disposition của rule; seeds còn lại giữ mapping codes/zone keys.
- [selectors.yml](selectors.yml): monthly DDS, batch tests và full audit riêng. Full audit cần `minimal_full_audit_enabled: true`, danh sách filename và expected count đúng catalog đã nạp; không chạy historical audit mỗi tháng.
- [build_with_detailed_tests.py](scripts/build_with_detailed_tests.py): in tên test, status và mô tả/kết quả chi tiết từ `run_results.json`; runner gọi sau mỗi lượt test.

Classification chỉ xử lý một batch/version dưới writer lock, cập nhật thống kê đầu vào và phân loại duplicate trong file. Hash serialization, DQ, KPI và lineage giữ nguyên. JSON nguồn chỉ dùng tạm để tính hash; không lưu workspace rộng thứ hai hoặc registry/lifecycle trong minimal runtime.

Với slim-v2, ANALYZE nhắm bảng unified và metadata thật thay vì view. Cleanup kiểm physical versions và yêu cầu các bảng rollback slim-v1 rỗng trước khi truncate danh sách bảng tường minh, cùng classification; không dùng CASCADE. Lỗi cleanup sau commit vẫn dùng retry-cleanup, giữ SUCCESS/watermark.

## Retry, lỗi và log

- Lỗi trước commit: batch không SUCCESS/watermark không tiến. `--resume-batch <id> --enable-cleanup` kiểm staging và dựng lại DDS của đúng version; chỉ dùng khi nguồn còn đầy đủ.
- Cleanup lỗi sau commit: dùng `--retry-cleanup <id>`, không nạp lại file đã thành công.
- Replacement checksum/version cần approval tường minh; không tự xóa dữ liệu cũ vì checksum đổi.
- Invocation artifacts/log và thời gian phase ở `data/work/task_4_4/dbt_artifacts/`; không commit chúng. Commit/cleanup/reporting có trạng thái riêng.

## Kiểm chứng và giới hạn

Regression tests bảo vệ contracts/runner được giữ ở `tests/`; công cụ benchmark/QA tạm và wrapper T09–T11 đã được dọn. Có thể chạy các kiểm tra không ghi database:

```powershell
./.venv-dbt-312/Scripts/python.exe -m unittest tests.test_backfill_safety_gates
./.venv-dbt-312/Scripts/python.exe tests/test_dbt_minimal_contract.py
```

Kết quả May thật sau tích hợp Mage main + DDS: Mage104.484s; build347.125s +tests96.406s +recon0.063s = **DDS443.594s**, vượt360s. Native Source→DDS wall555.982s (~9m16s) đạt≤600s ở lượt này, đã gồm overhead/commit/cleanup. Dữ liệu16models/15tests/đối soát/commit/cleanup/skip PASS. Chưa migrate live, chưa nghiệm thu đủ24 file, OLAP hoặc Power BI. [T10](../reports/dbt_minimal_refactor/T10.md), [T11](../reports/dbt_minimal_refactor/T11.md), [contract](../docs/etl_design/DBT_MINIMAL_CONTRACT.md).
