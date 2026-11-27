# Mage slim-v2: unified physical staging

Trạng thái 2026-11-27: **đã áp dụng live** trên PostgreSQL/PostGIS Docker và đã được dùng thành công từ Mage UI cho 2024-05 và 2025-05. Sau kiểm chứng, dữ liệu benchmark được reset; contract `slim-v2`, source và reference được giữ lại.

## Mục tiêu và phạm vi

`slim-v2` giảm thời gian của block `load_stage` xuống dưới 180 giây cho một
tháng đại diện/lớn, nhưng không thay đổi hợp đồng logic mà đề bài yêu cầu:

- Raw vẫn chứa toàn bộ dòng nguồn và lineage;
- Canonical vẫn chỉ chứa dòng qua kiểm tra kỹ thuật;
- lỗi ép kiểu vẫn giữ giá trị gốc trong `raw_payload`;
- reject kỹ thuật vẫn được ghi vào `dq.technical_quarantine`;
- batch, file version, checksum, schema version, source month và watermark vẫn
  do các bảng control quản lý;
- publication vẫn là một transaction: COPY, quarantine, kiểm tra, index và
  chuyển trạng thái `STAGED` cùng thành công hoặc cùng rollback;
- dbt tiếp tục đọc `stage.yellow_taxi_raw` và
  `stage.yellow_taxi_canonical`, không cần đổi nghĩa model.

## Thay đổi vật lý

Migration `010_slim_v2_unified_stage.sql` tạo một bảng UNLOGGED duy nhất:

`stage.yellow_taxi_trip_data`

Mỗi dòng nguồn chỉ được ghi một lần. Bảng chứa projection Raw, projection
Canonical, `cast_error_flags`, `raw_payload`, `technical_valid` và định danh
file/row. Metadata lặp lại theo hàng được lấy từ `stage.stage_file_batch` và
`control.source_file_version` qua hai compatibility view:

- `stage.yellow_taxi_raw`: mọi dòng vật lý;
- `stage.yellow_taxi_canonical`: `WHERE technical_valid`.

Hai bảng vật lý `yellow_taxi_raw_data` và `yellow_taxi_canonical_data` của
`slim-v1` được giữ rỗng để rollback có kiểm soát, không bị xóa bằng CASCADE.

## Đường ghi tối ưu

1. Parquet được đọc theo batch giới hạn bộ nhớ.
2. Chỉ `file_version_id`, `source_row_number` và `source_record_key` được lặp
   theo từng dòng; metadata file còn lại được kiểm tra một lần trong control.
3. Raw và Canonical được ghép thành một frame vật lý.
4. Các frame được serialize riêng nhưng truyền vào **một** PostgreSQL COPY
   liên tục, thay vì 150 COPY cho 75 phần Raw + Canonical.
5. Technical quarantine được sinh server-side từ phần `NOT technical_valid`.
6. Chỉ một cặp PK/unique index được dựng lại sau COPY.
7. Một scan vật lý đối soát count, scope, source identity và phép phân hoạch
   Canonical/quarantine.

Các tham số PostgreSQL chỉ có hiệu lực trong transaction của loader:

- `work_mem = 128MB`;
- `maintenance_work_mem = 512MB`;
- `effective_io_concurrency = 200`;
- `max_parallel_maintenance_workers = 2`.

Không tắt `fsync`, không tắt `synchronous_commit`, không bỏ kiểm tra cuối và
không cập nhật watermark tại block Mage.

## Migration và rollback

- Nâng cấp: `sql/migrations/010_slim_v2_unified_stage.sql`.
- Rollback khi staging rỗng: `sql/migrations/010_slim_v2_rollback.sql`.
- Cả hai script fail-closed nếu có batch đang hoạt động hoặc staging còn dữ
  liệu.
- Biến Mage phía người dùng vẫn là `stage_storage_mode: slim`; `slim-v2` là
  version hợp đồng vật lý trong database, không phải biến UI mới.

## Bằng chứng kiểm thử ngày 2026-11-27

Các benchmark chạy trên database QA tạm, dùng đúng pipeline Mage
`execute_sync`, `streaming + direct`, batch size 50.000; không gồm scheduler
wait, DDS hoặc reference initialization. Mỗi tháng dưới đây mới chạy một lần,
vì vậy chưa thay thế cổng nghiệm thu ba lần liên tiếp trên Mage UI.

| Tháng | Dòng nguồn | `load_stage` | Source→STAGED | Kết quả |
|---|---:|---:|---:|---|
| 2024-05 | 3.723.833 | 107,110 giây | 110,551 giây | PASS |
| 2025-05 | 4.591.845 | 125,800 giây | 131,577 giây | PASS |

Kiểm chứng UI sau rollout:

- 2024-05: người dùng ghi nhận khoảng 2 phút;
- 2025-05: chạy thành công; chưa cung cấp số giây chính xác nên không tự tạo số liệu;
- mỗi lượt kết thúc ở `STAGED`, đúng phạm vi pipeline ingestion hiện tại;
- dữ liệu hai lượt đã được reset trước khi bàn giao source.

Kiểm thử bổ sung đã PASS:

- migration và rollback fail-closed;
- fault sau unified COPY rollback cả dữ liệu và metadata;
- Raw count = Canonical count + technical quarantine count;
- source identity, lineage và original value của lỗi ép kiểu;
- dbt fixture 1.000 dòng cho kết quả Fact, tổng tiền và disposition giống
  `slim-v1`;
- `dbt run` hai view staging/intermediate: 2/2 PASS;
- `dbt test --select source:stage`: 17/17 PASS.

## Cổng nghiệm thu còn lại

Trước khi chốt SLA chính thức, chạy cùng tháng từ Mage UI trên database sạch
ba lần liên tiếp cho 2024-05 và 2025-05. Chỉ chấp nhận nếu mọi lần đều:

- `load_stage <= 180 giây`;
- không có retry/partial publication;
- toàn bộ dbt source/DDS/test và reconciliation PASS;
- watermark chỉ được commit sau khi chuỗi end-to-end thành công.
