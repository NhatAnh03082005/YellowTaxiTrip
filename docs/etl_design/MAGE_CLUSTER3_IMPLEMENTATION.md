# Cụm 3 Mage: streaming và staging gọn

Ngày triển khai ban đầu: 2026-11-26. Cập nhật live: 2026-11-27.

Trạng thái hiện tại: **đã áp dụng `direct + streaming + slim-v2` trên database Docker**. Cụm này không thay đổi DDS/OLAP, công thức nghiệp vụ, source key, technical/business DQ, LSET/CET hoặc điều kiện commit watermark.

## Kiến trúc đang chạy

```text
Parquet bất biến + catalog/checksum/schema registry
  → đọc Arrow theo 50.000 dòng
  → source identity + canonicalize + technical flags
  → một COPY liên tục vào stage.yellow_taxi_trip_data
  → derive technical quarantine server-side
  → một scan count/key/scope/partition
  → dựng lại PK + unique index
  → commit data + receipt + trạng thái STAGED cùng transaction
```

- Mỗi source row chỉ có một physical row.
- `stage.yellow_taxi_raw` là compatibility view của toàn bộ physical rows.
- `stage.yellow_taxi_canonical` là compatibility view với `technical_valid=true`.
- `dq.technical_quarantine` lưu đúng phần bù `technical_valid=false`, error flags và original payload.
- Metadata file bất biến nằm một lần trong `control.source_file_version`; quan hệ file/version với batch nằm trong `stage.stage_file_batch`.
- dbt vẫn đọc hai relation logic cũ, vì vậy grain và giao diện bàn giao không đổi.
- Source Parquet được kiểm footer, size, mtime và SHA-256 trước commit.

## Cấu hình Mage

```yaml
prepare_mode: streaming
stage_load_mode: direct
stage_storage_mode: slim
source_batch_size: 50000
canonical_batch_size: 50000
run_type: BACKFILL
```

`slim-v2` là version contract trong PostgreSQL, không phải giá trị biến Mage. UI và runner vẫn truyền `stage_storage_mode=slim`.

`run_pipeline_in_one_process` vẫn để `false` trong metadata UI. Benchmark QA `execute_sync` không được dùng để khẳng định scheduler UI không có process gap; SLA `load_stage` và tổng pipeline phải báo riêng.

## Migration

1. `008_mage_raw_cast_payload.sql`: giữ original value của lỗi cast.
2. `009_slim_stage_optin.sql`: chuẩn hóa metadata và tạo contract `slim-v1` trung gian.
3. `010_slim_v2_unified_stage.sql`: tạo unified physical table và thay nội dung hai compatibility view bằng `CREATE OR REPLACE VIEW`.

Rollback `010_slim_v2_rollback.sql` chỉ chạy khi staging rỗng và không có batch active. Guard fail-closed; không dùng `CASCADE` mù và không xóa volume để rollback.

## Transaction và recovery

Direct mode chỉ nhận đúng một file/version và target sạch. Loader lấy advisory lock và table lock `NOWAIT`; batch STAGED hoặc dirty target bị từ chối. COPY, technical quarantine, validation, index build, counters, receipt và chuyển STAGED cùng nằm trong một transaction.

Lỗi trước commit rollback cả dữ liệu và DDL index. Mất phản hồi sau commit được nhận diện bằng receipt + actual counts, không COPY lần hai. UNLOGGED loss sau crash làm actual-count verification fail; receipt một mình không đủ chứng minh staging còn dữ liệu.

Pipeline ingestion dừng ở STAGED. dbt/DDS, reconciliation, commit watermark và cleanup phải do orchestration sau Mage thực hiện. Vì vậy không được chạy tháng tiếp theo khi batch trước còn STAGED, trừ reset benchmark có chủ đích.

## Benchmark và kiểm chứng

QA full-month, nguồn local, chunk 50.000, không gồm scheduler wait, DDS hoặc reference initialization:

| Tháng | Dòng | `load_stage` | Source → STAGED |
|---|---:|---:|---:|
| 2024-05 | 3.723.833 | 107,110 giây | 110,551 giây |
| 2025-05 | 4.591.845 | 125,800 giây | 131,577 giây |

Kiểm chứng bổ sung:

- migration/rollback và fault injection PASS;
- Raw = Canonical + Technical và source identity PASS;
- dbt fixture slim-v1/slim-v2 parity về Fact, totals và disposition;
- `dbt run` staging/intermediate 2/2 PASS;
- `dbt test --select source:stage` 17/17 PASS;
- người dùng chạy UI 2024-05 khoảng 2 phút và chạy thành công 2025-05.

Hai lượt UI trên chưa thay cổng ba lần lặp cùng điều kiện. Chưa có bằng chứng tổng Source → DDS ≤ 10 phút hoặc lịch 24 tháng hoàn chỉnh.
