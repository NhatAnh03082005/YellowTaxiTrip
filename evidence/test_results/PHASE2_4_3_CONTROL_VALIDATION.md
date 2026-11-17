# Phase 2 — Mục 4.3 Control Metadata Validation

**Ngày chạy:** 2026-11-17  
**Nhánh:** `feature/control-metadata-watermark`  
**Database:** `nyc_taxi` / PostgreSQL 16.9 / PostGIS 3.5  
**Kết quả chung:** PASS

## Môi trường

- PostgreSQL container: healthy, host port `5433`, container port `5432`.
- Mage container: running, port `6789`.
- Control objects: 5 tables, 18 functions, 15 explicit indexes.
- Test data dùng prefix `qa_43_`; không sử dụng pipeline production.

## Kết quả theo bước

| Bước | Test | Kết quả | Evidence chính |
|---|---|---|---|
| 14 | Chạy lại DDL, indexes, functions và seed | PASS | Tables/indexes dùng `IF NOT EXISTS`; 18 functions replace thành công; seed `INSERT 0 0` |
| 15 | Schema, functions, indexes, comments | PASS | `PASS T15: 5 tables, 18 functions, indexes and table comments` |
| 16 | BACKFILL/INCREMENTAL/discovery completeness | PASS | `PASS T16`; incomplete catalog bị từ chối; known identity ngoài window bị loại; SHA mới/file chưa SUCCESS được chọn |
| 17 | Empty batch + watermark success/failure | PASS | `PASS T17A/T17B/T17C`; batch SUCCESS tiến LSET=CET; batch FAILED giữ version 0 |
| 18 | File lifecycle/checksum approval/retry | PASS | `PASS T18/T18B/T18C/T18D`; skip known SHA, stop SHA mới, approval audit đủ, retry chain đúng |
| 19 | Concurrency + heartbeat/STALE | PASS | Session B chờ 3.4 giây rồi bị active-batch gate chặn; `PASS T19B` |
| 20 | Required-step/reconciliation gates | PASS | `PASS T20`; missing step, raw mismatch và canonical mismatch đều bị chặn |
| 21 | Mage batch-context propagation | PASS | `pl_control_smoke_validation` completed; cùng `batch_id = 21` đi qua 3 block |

## Concurrency evidence

Hai PostgreSQL session thật được chạy đồng thời cho cùng pipeline/source:

```text
Session A: BEGIN -> start_batch -> pg_sleep(4) -> COMMIT
Session B: chờ advisory transaction lock 3.4 giây
Session B: ERROR Active or STALE batch already exists
```

Chỉ batch `14` được tạo; sau assertion batch được đóng với `QA_TEST_COMPLETED`.

## Mage evidence

Pipeline smoke test:

```text
start_control_smoke_batch
  -> assert_control_smoke_batch
  -> finish_control_smoke_batch
```

- Block đầu commit batch trước khi trả context.
- Block giữa đọc lại đúng batch `21` từ PostgreSQL.
- Block cuối đóng batch bằng `MAGE_SMOKE_TEST_COMPLETED`.
- Watermark `qa_43_t21_mage` giữ version `0`, đúng vì smoke batch kết thúc có chủ đích ở FAILED.

## Persistence evidence

Sau `docker compose restart postgres`:

```text
persisted_qa_batches = 19
max_batch_id         = 21
control_tables       = 5
postgres status      = healthy
```

Restart không làm mất Control metadata.

## Trạng thái cuối

```text
active QA batches = 0
```

Các batch FAILED trong tập QA là kết quả có chủ đích của negative tests, không phải lỗi chưa xử lý. Các batch SUCCESS đại diện cho positive paths và atomic watermark commit.

## Test files

- `tests/control/test_control_schema.sql`
- `tests/control/test_backfill_incremental.sql`
- `tests/control/test_empty_batch.sql`
- `tests/control/test_watermark_success.sql`
- `tests/control/test_watermark_failure.sql`
- `tests/control/test_file_lifecycle.sql`
- `tests/control/test_retry_idempotency.sql`
- `tests/control/test_file_version_change.sql`
- `tests/control/test_source_version_approval.sql`
- `tests/control/test_stale_batch.sql`
- `tests/control/test_concurrent_batch.sql`
- `tests/control/test_step_gate.sql`

## Kết luận

Control Plane đáp ứng các gates đã chốt cho mục 4.3: LSET/CET, discovery completeness, lifecycle file, source-version approval, retry contract, concurrency, stale recovery, required steps, reconciliation và atomic watermark commit. Mage đã kết nối và truyền cùng một batch context qua pipeline skeleton.
