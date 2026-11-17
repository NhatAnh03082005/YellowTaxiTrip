# NYC Yellow Taxi BI Project

Đây là repository thống nhất cho đồ án NYC Yellow Taxi BI. Team chọn các file
nguồn năm 2024–2025; đây là phạm vi nạp file, không phải điều kiện loại chuyến
theo ngày pickup.

## Trạng thái hiện tại

- Đã khảo sát dữ liệu, lập hợp đồng bàn giao và xây dựng phần dbt cho staging,
  chất lượng dữ liệu, DDS Fact/Dimensions và kiểm thử trên dữ liệu mẫu.
- Docker/PostGIS và pipeline kiểm tra kết nối Mage đã có; pipeline nạp file taxi,
  nạp đủ 24 file, OLAP và Power BI chưa được kiểm chứng hoàn chỉnh.
- Hướng dẫn và giới hạn cụ thể của dbt nằm trong [dbt_project/README.md](dbt_project/README.md).

## Thư mục chính

- `profiling/`: scripts và kết quả khảo sát dữ liệu đã hoàn thành ở giai đoạn 1.
- `docs/`: yêu cầu, data modeling, ETL design, decision log và checkpoint.
- `data/`: dữ liệu tham chiếu, landing và quarantine; không commit dữ liệu trip dung lượng lớn.
- `sql/`: nơi dự kiến đặt DDL cho control, audit, staging, DQ và DDS.
- `docker/` và `docker-compose.yml`: cấu hình container Mage và PostgreSQL/PostGIS.
- `mage_project/`: có pipeline kiểm tra kết nối; ingestion, backfill và incremental
  vẫn là phần tiếp theo.
- `dbt_project/`: staging, intermediate, Dimensions, Fact và tests.
- `powerbi/` và `olap/`: nơi dự kiến đặt semantic model, report và truy vấn phân tích.
- `tests/` và `evidence/`: test fixtures, reconciliation và bằng chứng chạy.
- `deployment/`: cấu hình, schedule, runbook và recovery.
- `reports/`: báo cáo theo các checkpoint.
- `submission/`: snapshot đóng gói để nộp, không phải source project độc lập.

Xem phân công, thứ tự triển khai và checklist chi tiết tại `KE_HOACH_DU_AN.md`.
