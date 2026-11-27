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

Hướng dẫn cấu hình, chạy một tháng, retry/skip và full audit: [dbt_project/README.md](dbt_project/README.md). Không dùng `dbt build` toàn project thay runner cho nạp dữ liệu tối giản.

## Trạng thái bàn giao

- DDS minimal, batch tests, reconciliation, skip/retry/cleanup và migration QA đã được kiểm chứng. Chưa migrate live.
- Lượt May thật mới nhất: 16 models/15 tests/đối soát/commit/cleanup PASS; DDS **411,906 giây**, chưa đạt mục tiêu360giây. [Kết quả đo](reports/dbt_minimal_refactor/T10.md), [tối ưu DDS](reports/dbt_minimal_refactor/T11.md).
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
