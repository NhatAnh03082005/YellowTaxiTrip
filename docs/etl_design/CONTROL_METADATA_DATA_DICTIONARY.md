# Control metadata data dictionary

**Owner:** Project Lead/Integration

**Scope:** Mục 4.3–4.5, không phụ thuộc code dbt trước checkpoint 4.6.

## 1. Quyết định triển khai

- Giữ Extraction Window `LSET < source_updated_at <= CET` cho INCREMENTAL.
- BACKFILL duyệt toàn bộ catalog trong scope, không loại file vì Last-Modified cũ.
- INCREMENTAL kết hợp Extraction Window với file history trong cùng grain
  `(pipeline_name, source_name)` để không bỏ sót file chưa từng SUCCESS hoặc file
  đổi checksum; lịch sử QA/pipeline khác không được làm nhiễm production.
- File lifecycle tách `DISCOVERED -> DOWNLOADING -> VALIDATED`.
- Retry sau downstream failure dùng `REPROCESS_IDEMPOTENTLY`, không tái sử dụng Staging cũ trong phiên bản đầu.
- Step-level evidence dùng `control.etl_step_run`.
- Active batch dùng heartbeat 15 phút và trạng thái `STALE`; batch STALE vẫn chặn run mới cho đến khi operator resolve.
- Source version đổi checksum phải được phê duyệt thủ công trước khi process.
- Empty batch chỉ thành công khi discovery kiểm tra đủ catalog.
- dbt Owner không review hoặc dùng Control runtime trước 4.6.

## 2. Time semantics

| Thuật ngữ | Định nghĩa |
|---|---|
| LSET | `last_successful_extraction_time` đã commit của pipeline/source |
| CET | `clock_timestamp()` được capture đúng một lần khi `start_batch` |
| Business time | Pickup/dropoff timestamp trong dữ liệu, không dùng làm control watermark |
| Source time | `source_last_modified`, dùng cho Extraction Window và audit |

Mọi control timestamp dùng `timestamptz` và lưu UTC.

## 3. Run types

| Run type | Discovery policy |
|---|---|
| BACKFILL | Duyệt toàn bộ catalog trong scope |
| INCREMENTAL | Window hoặc file chưa SUCCESS hoặc checksum khác history |
| RETRY | Tạo batch mới có `parent_batch_id`; reprocess idempotently |
| TEST | Dùng cho automated control tests |

## 4. Batch state machine

Luồng thành công:

```text
RUNNING -> STAGED -> DBT_RUNNING -> RECONCILING -> SUCCESS
```

Luồng lỗi:

```text
active state -> FAILED
active state -> STALE -> FAILED/CANCELLED
```

Active statuses gồm `CREATED`, `RUNNING`, `STAGED`, `DBT_RUNNING`, `RECONCILING`, `STALE`.

## 5. File state machine

```text
DISCOVERED
  -> DOWNLOADING
  -> VALIDATED
     -> STAGED
     -> SKIPPED_ALREADY_PROCESSED
     -> SOURCE_VERSION_CHANGED
        -> APPROVED_SOURCE_VERSION
  -> FAILED
```

SHA-256 được phép null trước validation. Từ VALIDATED trở đi phải có content length, SHA-256, schema fingerprint và schema version.

## 6. Step state machine

```text
PENDING -> RUNNING -> SUCCESS/FAILED/SKIPPED
```

Required steps:

1. `DISCOVERY`;
2. `FILE_VALIDATION`;
3. `STAGING`;
4. `DBT_BUILD_DDS`;
5. `DBT_TEST`;
6. `RECONCILIATION`;
7. `DBT_BUILD_OLAP`.

Trong 4.3, downstream dbt steps được mô phỏng trong tests. Từ 4.6, Mage ghi kết quả thật.

## 7. Reconciliation counters

| Counter | Ý nghĩa |
|---|---|
| `rows_raw` | Tổng dòng đọc ở raw boundary |
| `rows_canonical` | Dòng qua technical canonicalization |
| `rows_technical_quarantined` | Dòng technical quarantine |
| `rows_business_quarantined` | Dòng business quarantine |
| `rows_duplicate_quarantined` | Duplicate loser trong cùng file |
| `rows_fact_loaded` | Dòng accepted/warning vào Fact |
| `rows_amount_reconciliation_eligible` | Dòng đủ điều kiện amount reconciliation |

Success gate kiểm tra:

```text
rows_raw = rows_canonical + rows_technical_quarantined

rows_canonical
= rows_fact_loaded
 + rows_business_quarantined
 + rows_duplicate_quarantined
```

## 8. Discovery completeness

Empty batch chỉ hợp lệ khi:

```text
discovery_status = SUCCESS
catalog_checked_count = catalog_expected_count
files_selected = 0
```

Với scope hiện tại, expected catalog count là 24. Discovery lỗi hoặc kiểm tra thiếu file không được phép commit watermark.

## 9. Retry contract

Khi Staging thành công nhưng downstream thất bại:

1. batch hiện tại chuyển FAILED;
2. watermark giữ nguyên;
3. retry tạo batch mới với `parent_batch_id`;
4. file action là `REPROCESS_IDEMPOTENTLY`;
5. 4.5 phải dùng `source_record_key` unique và `ON CONFLICT` để không duplicate.

Không sử dụng `REUSE_STAGED` trong phiên bản đầu.

## 10. Source-version approval

Nếu cùng filename có SHA mới:

1. file chuyển `SOURCE_VERSION_CHANGED`;
2. approval status `PENDING`;
3. batch dừng và watermark không đổi;
4. operator gọi `approve_source_version` với người duyệt và lý do;
5. batch mới/retry mới được phép process SHA đã duyệt.

## 11. Security và audit

- Không ghi password, `.env` hoặc connection string chứa secret vào log/details.
- `etl_log` append-only theo contract ứng dụng.
- Mọi source-version approval lưu người duyệt, thời gian và lý do.
- Mọi retry liên kết batch gốc bằng `parent_batch_id`.
