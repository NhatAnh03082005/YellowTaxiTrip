# Evidence — Task A reference data và test fixtures

**Ngày kiểm tra:** 2026-11-16 (Asia/Bangkok)

## Taxi Zone reference

| Kiểm tra | Kết quả |
|---|---:|
| Taxi Zone lookup rows | 265 |
| Unique LocationID | 265 |
| LocationID range | 1–265 |
| Shapefile DBF records/geometries | 263 |
| `.shp`, `.shx`, `.dbf`, `.prj` đầy đủ | Pass |

LocationID 264 và 265 không có polygon là expected theo Design Freeze, không phải lỗi giải nén.

## Representative fixtures

| Fixture | Rows | Kích thước |
|---|---:|---:|
| `yellow_tripdata_2024-05_sample.parquet` | 5,000 | 137,422 bytes |
| `yellow_tripdata_2025-05_sample.parquet` | 5,000 | 141,597 bytes |
| `business_rule_cases.parquet` | 9 | 7,037 bytes |
| `technical_cast_errors.csv` | 5 | 451 bytes |

Hai sample Parquet được chọn bằng seed cố định qua tất cả row group, giữ schema nguồn tương ứng 2024/2025 và có coverage cho null, code đặc biệt, Zone 264/265, amount/distance không dương và CBD fee.

Kết quả `python tests/validate_fixtures.py`: `Fixture validation passed`.

## Portability và security scan

- Không tìm thấy đường dẫn `D:\...`, `C:\Users\...`, `/Users/...` hoặc `/home/...` trong executable source/config.
- Không phát hiện GitHub token, AWS access key, private key hoặc PostgreSQL URL chứa password.
- 24 Parquet nguồn lớn vẫn bị `.gitignore` loại khỏi Git.
