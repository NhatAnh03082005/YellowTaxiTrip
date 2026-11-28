# NYC Yellow Taxi BI

Repository đồ án NYC Yellow Taxi. Team chọn các file nguồn 2024–2025; đây là phạm vi nạp file, không phải điều kiện loại chuyến theo ngày pickup.

## Luồng dữ liệu hiện tại

```text
Parquet + catalog/checksum + schema registry
  → Mage: raw / canonical / technical quarantine
  → dbt: chuẩn hóa, enrichment, fingerprint, classification
  → DDS Fact + 6 Dimensions + business/duplicate quarantine
  → batch tests + đối soát
  → commit SUCCESS/watermark
  → cleanup staging/workspace

DDS được nghiệm thu → job reporting/OLAP riêng → Power BI
```

Điểm điều phối Source → DDS: [run_24_month_backfill.py](deployment/scripts/run_24_month_backfill.py). Script gọi các hàm block Mage và điều phối dbt; không phải master DAG/scheduler Mage đã được nghiệm thu live. Entry point incremental: [run_dds_incremental.py](deployment/scripts/run_dds_incremental.py).

Nhánh tích hợp lấy Mage từ `origin/main` (`5c456e7`): `direct + streaming + slim-v2`, một bảng vật lý unified và hai view Raw/Canonical. [Mage stage contract](docs/etl_design/MAGE_STAGE_CONTRACT.md). DDS cập nhật thống kê và cleanup theo physical storage contract; giữ riêng các gate tests/reconciliation/watermark của DDS.

Hướng dẫn cấu hình, chạy một tháng, retry/skip và full audit: [dbt_project/README.md](dbt_project/README.md). Không dùng `dbt build` toàn project thay runner cho nạp dữ liệu tối giản.

## Trạng thái bàn giao

- DDS minimal, batch tests, reconciliation, skip/retry/cleanup và migration QA đã được kiểm chứng. Chưa migrate live.
- Lượt May thật sau tích hợp Mage main + DDS: 16 models/15 tests/đối soát/commit/cleanup/skip PASS; tổng Source→DDS **555,982 giây (~9m16s)**, Mage104,484s, DDS443,594s. Tổng đạt≤600s ở lượt này, DDS chưa đạt360s. [Kết quả đo](reports/dbt_minimal_refactor/T10.md), [tối ưu DDS](reports/dbt_minimal_refactor/T11.md).
- Lượt June trên database chính cục bộ đã có May:3.539.193raw,16models/15tests/audit2file/commit/cleanup PASS; Mage109.328s, DDS751.390s, Source→DDS880.047s (~14m40s), chưa đạt timing. Watermark2, May counts/money/identity checksum giữ nguyên. [Báo cáo June](reports/dbt_minimal_refactor/T11.md).
- Đợt tối ưu DDS ngày28/11: raw scalar/policy/Fact test đo trọn May→June **869.063s (~14m29s)**, strict output parity PASS. DDS347.312/368.531s: June còn vượt360s8.531s. Bản partition tiếp theo PASS regression/probe nhưng chưa đo xong toàn luồng. [Kết quả và giới hạn](reports/dbt_minimal_refactor/T11.md).
- Theo yêu cầu dọn C ngày28/11, đã xóa dữ liệu PostgreSQL của đồ án gồm main/QA trong volume `nyc_taxi_bi_postgres_data` và container PostgreSQL cũ. Các trạng thái database trong báo cáo trước đó là lịch sử. Code, Parquet/logs trên D giữ nguyên; lần chạy sau cần khởi tạo/nạp lại. Docker đã dừng, không tự nạp lại sau cleanup. T10/T11 chưa DONE.
- Sau cleanup, đã khởi động lại Mage/PostgreSQL và triển khai cấu trúc control/staging cùng slim-v2 trên database mới. Pipeline kết nối và reference đã chạy PASS (265lookup/263geometry); trips0, batches0, watermark0 tại thời điểm kiểm tra. Đã sửa ba block reference để giữ decorator của Mage; regression kiểm đăng ký19block/4pipeline PASS. Chưa nạp lại dữ liệu tháng hoặc benchmark DDS. [Lưu ý database mới](docs/etl_design/MAGE_STAGE_CONTRACT.md).
- OLAP/reporting tách khỏi lượt nạp DDS; T07 chưa nghiệm thu. Chưa xác nhận hoàn tất toàn bộ24 file hoặc refresh Power BI.
- Công cụ benchmark/QA T09–T11, lịch QA tạm và bản task plan trùng đã được dọn khỏi source. Log/snapshot/dữ liệu QA cục bộ ở `data/work` được Git bỏ qua; database và file nguồn giữ nguyên.

## Cấu trúc

| Thư mục | Vai trò |
| --- | --- |
| `mage_project/` | Ingestion, checksum/schema, chuẩn hóa kiểu và staging |
| `dbt_project/` | DDS models, rules/seeds, tests và báo cáo chi tiết test |
| `sql/` | Control/staging DDL, functions, migrations và rollback |
| `deployment/scripts/` | Controller backfill và entrypoint incremental |
| `docs/` | Contract, thiết kế, mapping, kế hoạch và migration guide |
| `tests/` | Regression tests, fixture/oracle và SQL checks được giữ để bảo vệ luồng chính |
| `data/catalog/` | Catalog và schema registry đầu vào |
| `reports/` | Kết quả khảo sát và trạng thái bàn giao |
| `olap/`, `powerbi/` | Phần reporting/phân tích, triển khai riêng |

Kế hoạch chính: [DBT_MINIMAL_REFACTOR_TASK_PLAN.md](docs/etl_design/DBT_MINIMAL_REFACTOR_TASK_PLAN.md). Phân công đồ án: [KE_HOACH_DU_AN.md](KE_HOACH_DU_AN.md).
