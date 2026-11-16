# Kế hoạch triển khai mục 4.3 — Control, metadata và watermark

**Owner:** Project Lead/Integration

**Reviewer:** dbt Owner

**Trạng thái:** READY TO IMPLEMENT

**Điều kiện đầu vào:** Task A hoàn thành; Docker/PostgreSQL/PostGIS/Mage hoàn thành; cả hai thành viên kết nối PostgreSQL thành công.

## 1. Mục tiêu

Xây control plane dùng chung cho Mage và dbt để mỗi lần chạy ETL có thể:

- xác định chính xác cửa sổ incremental `LSET < source_updated_at <= CET`;
- ghi nhận batch, file, checksum, schema version, counters và lỗi;
- retry an toàn mà không nạp trùng;
- không cập nhật watermark khi ingestion, dbt hoặc reconciliation thất bại;
- truy vết từ một dòng Staging/Fact về batch và source file;
- cung cấp evidence vận hành bằng SQL thay vì chỉ dựa vào ảnh chụp thủ công.

## 2. Phạm vi và ranh giới

### Thuộc mục 4.3

- `control.etl_watermark`;
- `control.etl_batch`;
- `control.etl_file`;
- `control.etl_log`;
- indexes, constraints và functions/procedures phục vụ lifecycle batch;
- test SQL cho watermark, retry, concurrency và failure;
- contract để Mage ghi và dbt đọc metadata.

### Chưa thuộc mục 4.3

- tải/nạp Parquet thật vào Staging — mục 4.4/4.5;
- business DQ — dbt;
- Dimensions/Fact — dbt;
- reconciliation business amounts — mục 4.6/5.7;
- lịch schedule production — mục 4.8.

## 3. Quy ước đã chốt cho implementation

### 3.1. Time và cửa sổ extraction

- Mọi timestamp control dùng `timestamptz` và lưu UTC.
- `LSET`: successful extraction time gần nhất đã commit.
- `CET`: current extraction time, được capture đúng một lần khi tạo batch.
- Predicate incremental:

```text
LSET < source_updated_at <= CET
```

- Không gọi lại `now()` ở từng block để tạo CET mới.
- Batch đầu tiên dùng `initial_lset` được truyền rõ; không âm thầm dùng thời gian hiện tại.
- Backfill 2024–2025 dùng cửa sổ được truyền bởi runtime variables, nhưng vẫn ghi LSET/CET trong batch.

### 3.2. Watermark grain

Một watermark cho mỗi cặp:

```text
(pipeline_name, source_name)
```

Khuyến nghị ban đầu:

```text
pipeline_name = pl_taxi_master
source_name   = yellow_taxi_tripdata
```

Reference CSV và Shapefile dùng source/pipeline riêng, không chia sẻ watermark với Yellow Taxi.

### 3.3. Batch và file identity

- `batch_id`: identity/bigint do PostgreSQL sinh, không dùng timestamp làm khóa.
- `source_file_sha256`: identity của đúng nội dung file.
- `source_record_key = SHA256(source_file_sha256 || ':' || source_row_number)` chống duplicate khi rerun.
- `etl_file` ghi một file attempt trong một batch; uniqueness tối thiểu là `(batch_id, source_file_sha256)`.
- Cùng filename nhưng SHA-256 đổi phải dừng ở `SOURCE_VERSION_CHANGED`, không tự ghi đè metadata cũ.

### 3.4. Trạng thái

Không dùng PostgreSQL ENUM ở phiên bản đầu để migration dễ hơn. Dùng `varchar` với `CHECK`.

Batch status:

```text
CREATED
RUNNING
STAGED
DBT_RUNNING
RECONCILING
SUCCESS
FAILED
CANCELLED
```

File status:

```text
DISCOVERED
DOWNLOADING
VALIDATED
STAGED
SKIPPED_ALREADY_PROCESSED
SOURCE_VERSION_CHANGED
FAILED
```

Log level:

```text
DEBUG
INFO
WARNING
ERROR
```

## 4. Thiết kế bốn bảng

## 4.1. `control.etl_watermark`

Grain: một dòng cho mỗi `(pipeline_name, source_name)`.

Các cột dự kiến:

| Cột | Type | Quy tắc |
|---|---|---|
| `pipeline_name` | `varchar(100)` | PK phần 1, not null |
| `source_name` | `varchar(100)` | PK phần 2, not null |
| `last_successful_extraction_time` | `timestamptz` | LSET đã commit |
| `last_successful_batch_id` | `bigint` | FK batch thành công gần nhất |
| `watermark_version` | `bigint` | tăng khi commit để optimistic audit |
| `created_at` | `timestamptz` | default `clock_timestamp()` |
| `updated_at` | `timestamptz` | cập nhật khi commit |

Constraints:

- primary key `(pipeline_name, source_name)`;
- batch được tham chiếu phải tồn tại;
- watermark chỉ được cập nhật qua success function, Mage không chạy `UPDATE` trực tiếp.

## 4.2. `control.etl_batch`

Grain: một lần thực thi pipeline.

Các nhóm cột:

### Identity và context

- `batch_id bigint generated always as identity primary key`;
- `pipeline_name varchar(100) not null`;
- `source_name varchar(100) not null`;
- `run_type varchar(20)` — `BACKFILL`, `INCREMENTAL`, `RETRY`, `TEST`;
- `parent_batch_id bigint null` — batch gốc khi retry;
- `triggered_by varchar(100)` — manual, schedule hoặc user/service.

### Extraction window

- `lset timestamptz not null`;
- `cet timestamptz not null`;
- check `lset <= cet`.

### Lifecycle

- `status varchar(24) not null`;
- `started_at timestamptz not null`;
- `ended_at timestamptz null`;
- `error_code varchar(100) null`;
- `error_message text null`;

### Counters

- `files_discovered`;
- `files_validated`;
- `rows_read`;
- `rows_staged`;
- `rows_technical_rejected`;
- `rows_business_rejected`;
- `rows_loaded`.

Tất cả counters dùng `bigint`, default 0 và check `>= 0`.

## 4.3. `control.etl_file`

Grain: một source file attempt trong một batch.

Các cột dự kiến:

- `etl_file_id bigint generated always as identity primary key`;
- `batch_id bigint not null` FK `etl_batch`;
- `source_file varchar(255) not null`;
- `source_url text`;
- `source_year_month char(7)` với format `YYYY-MM`;
- `source_last_modified timestamptz`;
- `content_length bigint`;
- `source_file_sha256 char(64) not null`;
- `schema_fingerprint char(64)`;
- `schema_version varchar(20)`;
- `file_status varchar(32) not null`;
- `rows_read`, `rows_staged`, `rows_rejected` bigint default 0;
- `discovered_at`, `validated_at`, `staged_at` timestamptz;
- `error_message text`;
- unique `(batch_id, source_file_sha256)`.

Indexes cần có:

- `(source_file, source_file_sha256)`;
- `(file_status, source_year_month)`;
- `batch_id`;
- `source_last_modified`.

## 4.4. `control.etl_log`

Grain: một event append-only.

Các cột dự kiến:

- `log_id bigint generated always as identity primary key`;
- `batch_id bigint null` FK;
- `etl_file_id bigint null` FK;
- `pipeline_name varchar(100) not null`;
- `block_name varchar(150)`;
- `log_level varchar(10) not null`;
- `event_code varchar(100)`;
- `message text not null`;
- `details jsonb not null default '{}'`;
- `logged_at timestamptz not null default clock_timestamp()`.

Indexes:

- `(batch_id, logged_at)`;
- `(log_level, logged_at)`;
- GIN trên `details` chỉ thêm nếu truy vấn thực tế cần; không thêm sớm.

## 5. Functions/procedures cần triển khai

Tách DDL và lifecycle logic để Mage không tự ghép SQL rời rạc.

## 5.1. `control.start_batch(...)`

Trách nhiệm:

1. lấy advisory lock theo `(pipeline_name, source_name)`;
2. đọc/khởi tạo watermark;
3. capture CET một lần;
4. insert batch `RUNNING`;
5. trả `batch_id`, `lset`, `cet` cho Mage.

Nếu pipeline cùng source đang RUNNING, function phải fail rõ ràng; không tạo hai batch chồng cửa sổ.

## 5.2. `control.register_file(...)`

- insert file metadata;
- kiểm tra SHA-256 đã xử lý thành công;
- phát hiện cùng filename nhưng checksum đổi;
- trả action: `PROCESS`, `SKIP_ALREADY_PROCESSED` hoặc `STOP_SOURCE_VERSION_CHANGED`.

## 5.3. `control.log_event(...)`

- append log chuẩn hóa;
- nhận `jsonb details` cho stack trace/counters;
- tuyệt đối không ghi password hoặc connection string.

## 5.4. `control.update_file_result(...)`

- cập nhật status/timestamps/counters của file;
- không cho counter âm;
- chỉ cho phép transition hợp lệ.

## 5.5. `control.fail_batch(...)`

- đổi batch sang `FAILED`;
- ghi `ended_at`, error code/message;
- append error log;
- không cập nhật watermark.

## 5.6. `control.commit_batch_success(...)`

Chạy trong một transaction:

1. lock batch và watermark;
2. xác nhận batch đang `RECONCILING`;
3. xác nhận không có file `FAILED`/`SOURCE_VERSION_CHANGED`;
4. xác nhận dbt và reconciliation đã pass qua flags/log contract;
5. cập nhật batch `SUCCESS` và `ended_at`;
6. cập nhật `LSET = batch.CET` và tăng `watermark_version`;
7. commit cùng lúc.

Bất kỳ bước nào lỗi phải rollback cả batch success lẫn watermark.

## 6. File implementation dự kiến

```text
sql/control/
├── 001_control_tables.sql
├── 002_control_indexes.sql
├── 003_control_functions.sql
└── 004_control_seed_watermarks.sql

tests/control/
├── test_control_schema.sql
├── test_watermark_success.sql
├── test_watermark_failure.sql
├── test_retry_idempotency.sql
├── test_concurrent_batch.sql
└── test_file_version_change.sql

evidence/test_results/
└── PHASE2_4_3_CONTROL_VALIDATION.md
```

Init Docker chỉ tạo schema/extension. Control DDL chạy bằng migration command riêng để thay đổi SQL không buộc xóa volume.

## 7. Luồng Mage sử dụng control tables

```text
Mage get_extraction_window
  -> call control.start_batch
  -> nhận batch_id, LSET, CET

discover_source_files
  -> call control.register_file cho từng file

verify/load/stage
  -> update etl_file counters/status
  -> append etl_log events

dbt build + tests
  -> dùng cùng batch_id
  -> trả exit code/test result

reconciliation
  -> ghi result/log

all pass
  -> call control.commit_batch_success

any failure
  -> call control.fail_batch
  -> watermark giữ nguyên
```

Mage truyền `batch_id` xuống mọi block bằng runtime context; không block nào tự tạo batch mới.

## 8. Contract cho dbt Owner

dbt chỉ cần đọc:

- `batch_id` hiện tại từ Mage variables;
- `control.etl_batch.lset/cet` nếu cần audit;
- lineage columns trong Staging.

dbt không được:

- update watermark;
- tự mark batch `SUCCESS`;
- sửa file status;
- tạo batch ID khác cho cùng master run.

dbt trả cho Mage:

- command/selector;
- exit code;
- test result;
- reconciliation result;
- error message rút gọn và đường dẫn artifact/log.

## 9. Kế hoạch thực hiện theo thứ tự

### 4.3.1 — Chốt data dictionary

- review tên cột/type/nullable/default;
- review status transitions;
- review initial LSET cho backfill và incremental;
- dbt Owner xác nhận contract đọc batch metadata.

### 4.3.2 — Tạo tables và constraints

- viết `001_control_tables.sql`;
- chạy trên database sạch;
- chạy lại lần hai để chứng minh DDL idempotent;
- kiểm tra PK/FK/check constraints.

### 4.3.3 — Tạo indexes

- viết `002_control_indexes.sql`;
- dùng `EXPLAIN` cho lookup watermark, file checksum và batch log;
- không thêm index không có query use case.

### 4.3.4 — Tạo lifecycle functions

- start batch;
- register file;
- log/update result;
- fail batch;
- commit success/watermark.

### 4.3.5 — Tích hợp Mage skeleton

- tạo block `get_extraction_window`;
- tạo helper gọi control functions;
- truyền batch context xuống downstream;
- chưa cần load 24 file thật ở bước này.

### 4.3.6 — Positive tests

- first run;
- successful incremental batch;
- empty successful batch;
- restart PostgreSQL rồi đọc lại metadata;
- cùng input retry không tạo staging duplicate.

### 4.3.7 — Negative tests

- dbt exit code khác 0 → watermark không đổi;
- reconciliation fail → watermark không đổi;
- checksum đổi cùng filename → stop/review;
- hai batch cùng source chạy đồng thời → batch thứ hai bị chặn;
- invalid status transition → fail;
- counter âm → constraint fail.

### 4.3.8 — Evidence và bàn giao

- lưu SQL output/test results;
- chụp query batch/file/log/watermark;
- cập nhật `KE_HOACH_DU_AN.md` chỉ sau khi tests đạt;
- push branch và yêu cầu dbt Owner review contract.

## 10. Test matrix bắt buộc

| Test | Expected |
|---|---|
| Start first batch | Tạo watermark/batch, trả LSET/CET hợp lệ |
| Successful batch | Batch SUCCESS, LSET mới bằng CET |
| Failed ingestion | Batch FAILED, LSET không đổi |
| Failed dbt | Batch FAILED, LSET không đổi |
| Failed reconciliation | Batch FAILED, LSET không đổi |
| Empty batch | SUCCESS hợp lệ; watermark tiến nếu discovery hoàn tất |
| Retry failed batch | parent_batch_id được ghi; không duplicate |
| Same file/checksum | skip nếu đã SUCCESS |
| Same filename/new checksum | SOURCE_VERSION_CHANGED và stop |
| Concurrent start | Chỉ một batch giữ lock |
| PostgreSQL restart | Metadata còn nguyên |
| Re-run DDL | Không lỗi, không mất dữ liệu |

## 11. Definition of Done cho mục 4.3

- [ ] Có đủ bốn bảng và comments/data dictionary.
- [ ] PK, FK, unique, check constraints và indexes đã kiểm tra.
- [ ] `start_batch`, `register_file`, `fail_batch`, `commit_batch_success` chạy đúng.
- [ ] LSET/CET dùng `timestamptz` và CET chỉ capture một lần.
- [ ] Failure ở Mage/dbt/reconciliation không cập nhật watermark.
- [ ] Retry và cùng input không tạo duplicate.
- [ ] Concurrent batch cùng source bị chặn.
- [ ] DDL chạy lại an toàn trên database đã có metadata.
- [ ] Mage block skeleton nhận và truyền cùng `batch_id`.
- [ ] dbt Owner review contract metadata.
- [ ] Positive/negative test evidence được lưu và push.

Chỉ chuyển sang triển khai ingestion 4.4 sau khi các điều kiện trên đạt.
