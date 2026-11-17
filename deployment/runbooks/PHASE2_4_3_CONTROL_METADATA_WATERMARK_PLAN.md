# Kế hoạch triển khai mục 4.3 — Control, metadata và watermark

**Owner:** Project Lead/Integration

**Reviewer trước 4.6:** Project Lead tự kiểm thử và đối chiếu evidence

**Trạng thái:** BƯỚC 1–22 ĐÃ HOÀN THÀNH VÀ KIỂM THỬ; CHỜ COMMIT/PUSH BƯỚC 23

Project Lead toàn quyền triển khai 4.3–4.5. dbt Owner làm độc lập; chỉ tích hợp tại checkpoint 4.6.

## 1. Quyết định đã chốt

- Giữ Extraction Window `LSET < source_updated_at <= CET` cho INCREMENTAL; CET được capture đúng một lần.
- BACKFILL duyệt toàn bộ catalog trong scope.
- INCREMENTAL chọn file nằm trong window, chưa từng SUCCESS hoặc có checksum mới.
- File lifecycle tách `DISCOVERED -> DOWNLOADING -> VALIDATED`.
- Retry tạo batch mới có `parent_batch_id` và reprocess idempotently; chưa reuse Staging cũ.
- Batch mất heartbeat chuyển `STALE`; operator phải resolve trước khi có run mới.
- Cùng filename nhưng SHA-256 mới phải được duyệt thủ công.
- Empty batch chỉ SUCCESS khi discovery kiểm đủ catalog.
- Không cập nhật watermark nếu ingestion, dbt, required step hoặc reconciliation thất bại.

Data dictionary: `docs/etl_design/CONTROL_METADATA_DATA_DICTIONARY.md`.

## 2. Phạm vi

### Thuộc 4.3

- Năm bảng `control.etl_batch`, `control.etl_watermark`, `control.etl_file`, `control.etl_step_run`, `control.etl_log`.
- PK, FK, unique/check constraints, comments và indexes.
- Functions cho batch, file, step, log, discovery, stale recovery, approval và atomic watermark commit.
- SQL tests cho schema, success/failure, retry, concurrency, lifecycle, empty batch, stale batch, step gate và approval.
- Contract để Mage dùng cùng `batch_id` trên toàn pipeline.

### Chưa thuộc bước 1–13

- Chạy DDL trên PostgreSQL: bước 14.
- Chạy tests và lưu evidence: bước 15–22.
- Mage skeleton/runtime integration: bước 21.
- Nạp 24 Parquet vào Physical Staging: 4.4–4.5.
- Merge code dbt: checkpoint 4.6.

## 3. Time và identity semantics

- Control timestamp dùng `timestamptz`, lưu UTC.
- `LSET` là `last_successful_extraction_time` đã commit.
- `CET` là `clock_timestamp()` được capture một lần trong `control.start_batch`.
- Business pickup/dropoff time không dùng làm watermark.
- Watermark grain: `(pipeline_name, source_name)`.
- Batch identity: `batch_id bigint generated always as identity`.
- SHA-256 được phép null trước file validation.
- Record identity ở 4.5: `SHA256(source_file_sha256 || ':' || source_row_number)`.

Seed: `pl_taxi_master` / `yellow_taxi_tripdata`, initial LSET `1970-01-01T00:00:00Z`.

## 4. Data model và lifecycle

### `control.etl_batch`

Một lần chạy pipeline; lưu run type, retry parent, LSET/CET, heartbeat, discovery completeness, counters và error.

```text
RUNNING -> STAGED -> DBT_RUNNING -> RECONCILING -> SUCCESS
active -> FAILED
active -> STALE -> FAILED/CANCELLED
```

`STALE` vẫn chặn batch mới để tránh hai extraction window chồng nhau.

### `control.etl_watermark`

Một dòng cho mỗi pipeline/source. Chỉ `commit_batch_success` tiến LSET đến CET và tăng version.

### `control.etl_file`

Một file attempt trong batch; lưu source metadata, SHA/schema, lifecycle, counters, error và approval evidence.

```text
DISCOVERED -> DOWNLOADING -> VALIDATED -> STAGED
                                   |-> SKIPPED_ALREADY_PROCESSED
                                   |-> SOURCE_VERSION_CHANGED -> APPROVED_SOURCE_VERSION
any processing state -> FAILED
```

### `control.etl_step_run`

Step status: `PENDING -> RUNNING -> SUCCESS/FAILED/SKIPPED`.

Required steps: `DISCOVERY`, `FILE_VALIDATION`, `STAGING`, `DBT_BUILD_DDS`, `DBT_TEST`, `RECONCILIATION`, `DBT_BUILD_OLAP`.

### `control.etl_log`

Event log append-only theo application contract. Không ghi secret, password hoặc connection string.

## 5. Reconciliation và success gates

Counters gồm `rows_raw`, `rows_canonical`, `rows_technical_quarantined`, `rows_business_quarantined`, `rows_duplicate_quarantined`, `rows_fact_loaded`, `rows_amount_reconciliation_eligible`.

```text
rows_raw = rows_canonical + rows_technical_quarantined

rows_canonical
= rows_fact_loaded
 + rows_business_quarantined
 + rows_duplicate_quarantined
```

Success còn yêu cầu discovery SUCCESS, catalog checked bằng expected, không còn file unresolved, mọi required step hợp lệ và batch đang RECONCILING. Empty batch chỉ hợp lệ khi catalog được kiểm đủ và `files_selected = 0`.

## 6. Functions đã triển khai

| Function | Trách nhiệm |
|---|---|
| `log_event` | Ghi audit event |
| `start_batch` | Lock source, đọc/khởi tạo watermark, capture CET, tạo batch/steps |
| `touch_batch_heartbeat` | Gia hạn heartbeat |
| `mark_stale_batches` / `resolve_stale_batch` | Stale detection và operator recovery |
| `transition_batch` | Chặn state transition sai |
| `update_discovery_result` | Ghi discovery completeness |
| `update_batch_counters` | Ghi reconciliation counters |
| `should_discover_file` | Policy BACKFILL/INCREMENTAL/RETRY/TEST |
| `discover_file` / `mark_file_downloading` | File discovery/download lifecycle |
| `validate_file` | Trả PROCESS/SKIP/STOP/REPROCESS |
| `approve_source_version` | Ghi approval cho SHA mới |
| `start_step` / `finish_step` | Step evidence và exit code |
| `update_file_result` | File result/counters |
| `fail_batch` | Fail batch, giữ nguyên watermark |
| `commit_batch_success` | Kiểm gates và atomically commit SUCCESS + LSET=CET |

## 7. File implementation

```text
sql/control/
├── 001_control_tables.sql
├── 002_control_indexes.sql
├── 003_control_functions.sql
└── 004_control_seed_watermarks.sql

docs/etl_design/
└── CONTROL_METADATA_DATA_DICTIONARY.md

tests/control/
├── test_control_schema.sql
├── test_backfill_incremental.sql
├── test_watermark_success.sql
├── test_watermark_failure.sql
├── test_retry_idempotency.sql
├── test_concurrent_batch.sql
├── test_file_lifecycle.sql
├── test_empty_batch.sql
├── test_stale_batch.sql
├── test_step_gate.sql
├── test_file_version_change.sql
└── test_source_version_approval.sql
```

## 8. Mage contract

```text
start_batch -> batch_id/LSET/CET
discovery -> update_discovery_result + discover_file
download -> mark_file_downloading
validation -> validate_file
downstream -> start_step/finish_step + counters
all gates pass -> commit_batch_success
any failure -> fail_batch
```

Mage phải truyền cùng `batch_id` qua toàn pipeline.

## 9. Trình tự 23 bước

- [x] 1. Checkout nhánh `feature/control-metadata-watermark`.
- [x] 2. Tạo cấu trúc SQL, test và data dictionary.
- [x] 3. Chốt dictionary, nullable/default, lifecycle và ownership.
- [x] 4. Thiết kế `etl_batch` và heartbeat/STALE.
- [x] 5. Thiết kế `etl_watermark` và LSET/CET.
- [x] 6. Thiết kế `etl_file` với lifecycle ba tầng.
- [x] 7. Thiết kế `etl_step_run` và required steps.
- [x] 8. Thiết kế `etl_log` và audit contract.
- [x] 9. Tạo constraints và indexes.
- [x] 10. Tạo batch/discovery/file lifecycle functions.
- [x] 11. Tạo retry, heartbeat, stale recovery và approval.
- [x] 12. Tạo step gate, reconciliation và atomic success commit.
- [x] 13. Tạo seed watermark idempotent.
- [x] 14. Chạy DDL/seed trên PostgreSQL và chạy lại kiểm idempotency.
- [x] 15. Kiểm schema, constraints, comments và indexes.
- [x] 16. Test BACKFILL/INCREMENTAL/discovery completeness.
- [x] 17. Test success/failure watermark và empty batch.
- [x] 18. Test retry, file lifecycle và checksum change.
- [x] 19. Test concurrency, heartbeat/STALE và recovery.
- [x] 20. Test required step gate/reconciliation equations.
- [x] 21. Tích hợp Mage skeleton.
- [x] 22. Lưu evidence và cập nhật checklist 4.3.
- [ ] 23. Commit/push nhánh; chỉ merge dbt tại 4.6.

## 10. Definition of Done 4.3

- [x] Code có đủ năm bảng, comments, dictionary, constraints, indexes và lifecycle functions.
- [x] LSET/CET dùng `timestamptz`; CET capture một lần.
- [x] Có contract cho failure, retry, stale, empty batch và source-version approval.
- [x] DDL/seed chạy idempotent.
- [x] Positive/negative tests pass và PostgreSQL restart không mất metadata.
- [x] Mage skeleton nhận/truyền cùng `batch_id`.
- [x] Evidence và checklist 4.3 hoàn tất.
- [ ] Nhánh đã commit/push; chưa merge dbt trước 4.6.

Hoàn thành code bước 1–13 chưa đồng nghĩa database đã được migrate; chỉ đánh dấu toàn bộ 4.3 sau bước 14–23.
