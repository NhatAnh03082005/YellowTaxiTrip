# NYC Yellow Taxi BI Project

Đây là repository thống nhất dùng cho toàn bộ đồ án, không tách source theo từng giai đoạn.

## Tiến độ tích lũy

- Lần 1: khảo sát dữ liệu và thiết kế DDS.
- Lần 2: giữ toàn bộ lần 1 và bổ sung ETL bằng Mage, PostgreSQL/PostGIS và dbt trên Docker.
- Lần cuối: giữ toàn bộ kết quả trước đó và bổ sung OLAP, Power BI, kiểm thử, báo cáo và yêu cầu nâng cao.

## Thư mục chính

- `profiling/`: scripts và kết quả khảo sát dữ liệu đã hoàn thành ở giai đoạn 1.
- `docs/`: yêu cầu, data modeling, ETL design, decision log và checkpoint.
- `data/`: dữ liệu tham chiếu, landing và quarantine; không commit dữ liệu trip dung lượng lớn.
- `sql/`: DDL cho control, audit, staging, DQ và DDS.
- `docker/`: cấu hình container cho Mage, PostgreSQL/PostGIS và môi trường dbt.
- `mage_project/`: ingestion, orchestration, backfill và incremental pipelines.
- `dbt_project/`: staging, intermediate, Dimensions, Fact và tests.
- `powerbi/` và `olap/`: semantic model, report và truy vấn phân tích.
- `tests/` và `evidence/`: test fixtures, reconciliation và bằng chứng chạy.
- `deployment/`: cấu hình, schedule, runbook và recovery.
- `reports/`: báo cáo theo các checkpoint.
- `submission/`: snapshot đóng gói để nộp, không phải source project độc lập.

Xem phân công, thứ tự triển khai và checklist chi tiết tại `KE_HOACH_DU_AN.md`.
