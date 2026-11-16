# Evidence — tải và kiểm tra Yellow Taxi 2024–2025

**Ngày chạy:** 2026-11-16 (Asia/Bangkok)

**Script:** `profiling/scripts/download_validate_etl_files.py`

**Catalog:** `data/catalog/yellow_taxi_2024_2025.csv`

## Kết quả

| Kiểm tra | Kết quả |
|---|---:|
| Catalog rows | 24 |
| Unique year-month | 24 |
| Files present | 24 |
| Parquet footer/schema valid | 24 |
| File length matches catalog | 24 |
| SHA-256 hợp lệ | 24 |
| Schema fingerprint hợp lệ | 24 |
| HTTP status 200 | 24 |
| Có Last-Modified | 24 |
| Có downloaded_at | 24 |
| File `.tmp` Parquet còn lại | 0 |
| Tổng dung lượng | 1,522,975,012 bytes |

## Schema registry

| Schema version | Phạm vi | Số file |
|---|---|---:|
| `yt_schema_v001` | 2024-01 đến 2024-12 | 12 |
| `yt_schema_v002` | 2025-01 đến 2025-12 | 12 |

Schema version được tra theo SHA-256 của danh sách cột có thứ tự và logical types, không suy ra chỉ từ năm. Registry chi tiết nằm tại `data/catalog/yellow_taxi_schema_registry.csv`.

## Lưu trữ

24 Parquet được lưu cục bộ dưới `data/yellow_taxi/by_year_month/YYYY-MM/` và bị `.gitignore` loại khỏi Git. Catalog, schema registry, script tải/kiểm tra và evidence được commit; dữ liệu nguồn dung lượng lớn không được push.
