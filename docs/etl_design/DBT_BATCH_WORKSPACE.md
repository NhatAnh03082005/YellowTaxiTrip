# Tối ưu dbt cho luồng nạp tuần tự

## Cách chạy

Runner chính vẫn xử lý một file/version tại một thời điểm. Chạy đầy đủ dbt DAG
cho mỗi batch; không dùng `--resume-dbt-select` cho một batch mới.

`int_taxi_trip_fingerprinted` và `int_taxi_trip_classified` là bảng làm việc
`table`, dựng lại từ staging mỗi lần chạy, không append lịch sử. Điều này giữ
nguyên phạm vi staging của thiết kế hiện tại, bao gồm các fixture nhiều file.
Không thêm filter batch sớm vì có thể làm mất dòng liên quan khi phân loại trùng.

Fingerprint được tính một lần; phân loại dùng kết quả đó. Fact, DQ, audit và
165 test đọc cùng kết quả phân loại. Index theo source key và fingerprint hỗ trợ
join; `ANALYZE` sau khi tạo bảng cung cấp thống kê cho PostgreSQL.

Fact có index và thống kê trên bảng nguồn tạm và tập khóa mới. Công thức hash,
source key, quy tắc DQ, survivor, lifecycle và watermark không thay đổi.
`dim_date` lấy min/max pickup/dropoff trong một lần đọc tập accepted, vẫn mở rộng
theo ngày đã có và giữ hai thành viên kỹ thuật.

## Vòng đời và dung lượng

Hai bảng làm việc dùng chung vòng đời single-writer của staging. Cleanup của
runner chỉ truncate chúng sau batch SUCCESS, trong cùng transaction kiểm tra
batch và dọn staging. Lỗi hoặc `--no-cleanup` giữ lại dữ liệu để điều tra.
Các bảng Fact, DQ, lịch sử và metadata không thuộc cleanup này.

Đây là đánh đổi dung lượng đỉnh để giảm CPU, sort và file tạm lặp lại. Hai bảng
hiện còn chứa cột rộng phục vụ contract/lineage, nên phải đo peak thực tế trước
khi chạy 24 tháng. Chưa chuyển sang UNLOGGED; crash recovery vẫn theo PostgreSQL
thông thường. Chưa bỏ bảng fingerprint history hay đổi kiểu hash của Fact.

Khi retry, ưu tiên runner dựng lại toàn bộ DAG. Chỉ chọn model khi đã kiểm tra
dependency và xác nhận bảng làm việc thuộc đúng staging/version hiện tại.
Không xóa bảng làm việc giữa build và test.

## Kiểm chứng

Fixture SQL chạy trong transaction rollback trên PostgreSQL QA: golden hash
2024/2025, duplicate survivor, fingerprint history candidate, reconciliation và
OLAP. Unit test kiểm tra scope Fact và gate/cleanup của runner. `dbt parse` kiểm
tra cấu hình và dependency.

Chưa có benchmark một tháng sau thay đổi. Trước backfill cần chạy một tháng đầy
đủ với cleanup, retry cùng version, rồi thêm version 2025 để kiểm tra cross-file
warning. Ghi thời gian từng model, tất cả test, số dòng/số tiền, index, WAL, temp
và peak host disk. So baseline theo source key, hai fingerprint, disposition và
KPI; không coi parse hay fixture là bằng chứng hoàn tất 24 tháng.
