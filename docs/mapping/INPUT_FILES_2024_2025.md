# Input file list — Yellow Taxi 2024–2025

**Kiểm tra URL:** HTTP HEAD ngày 2026-11-15, cả 24 file trả HTTP 200.  
**Lưu ý:** trạng thái này xác nhận endpoint tồn tại; ingestion vẫn phải ghi SHA-256 sau download.

Base URL: `https://d37ci6vzurychx.cloudfront.net/trip-data/`

| Year-month | File | HEAD | Phạm vi phân tích |
|---|---|---:|---|
| 2024-01 | `yellow_tripdata_2024-01.parquet` | 200 | Full year, YoY |
| 2024-02 | `yellow_tripdata_2024-02.parquet` | 200 | Full year, YoY |
| 2024-03 | `yellow_tripdata_2024-03.parquet` | 200 | Full year, YoY |
| 2024-04 | `yellow_tripdata_2024-04.parquet` | 200 | Full year, YoY; profiled |
| 2024-05 | `yellow_tripdata_2024-05.parquet` | 200 | May comparison |
| 2024-06 | `yellow_tripdata_2024-06.parquet` | 200 | June comparison |
| 2024-07 | `yellow_tripdata_2024-07.parquet` | 200 | Full year, YoY |
| 2024-08 | `yellow_tripdata_2024-08.parquet` | 200 | Full year, YoY |
| 2024-09 | `yellow_tripdata_2024-09.parquet` | 200 | Full year, YoY |
| 2024-10 | `yellow_tripdata_2024-10.parquet` | 200 | Full year, YoY |
| 2024-11 | `yellow_tripdata_2024-11.parquet` | 200 | Full year, YoY |
| 2024-12 | `yellow_tripdata_2024-12.parquet` | 200 | Full year, YoY |
| 2025-01 | `yellow_tripdata_2025-01.parquet` | 200 | Full year, YoY; profiled; CBD begins |
| 2025-02 | `yellow_tripdata_2025-02.parquet` | 200 | Full year, YoY |
| 2025-03 | `yellow_tripdata_2025-03.parquet` | 200 | Full year, YoY |
| 2025-04 | `yellow_tripdata_2025-04.parquet` | 200 | Full year, YoY |
| 2025-05 | `yellow_tripdata_2025-05.parquet` | 200 | May comparison |
| 2025-06 | `yellow_tripdata_2025-06.parquet` | 200 | June comparison |
| 2025-07 | `yellow_tripdata_2025-07.parquet` | 200 | Full year, YoY |
| 2025-08 | `yellow_tripdata_2025-08.parquet` | 200 | Full year, YoY |
| 2025-09 | `yellow_tripdata_2025-09.parquet` | 200 | Full year, YoY |
| 2025-10 | `yellow_tripdata_2025-10.parquet` | 200 | Full year, YoY |
| 2025-11 | `yellow_tripdata_2025-11.parquet` | 200 | Full year, YoY |
| 2025-12 | `yellow_tripdata_2025-12.parquet` | 200 | Full year, YoY |

Reference inputs:

- `data/reference/taxi+_zone_lookup.csv` — 265 LocationID.
- `data/reference/taxi_zones.zip` — 263 geometries; 264/265 không có polygon là expected.

File acceptance checklist:

1. HTTP 200/206 và content length > 0.
2. Tên file khớp `yellow_tripdata_YYYY-MM.parquet`.
3. Parquet footer đọc được.
4. Tính schema fingerprint từ ordered column names + logical types và tra schema-version registry; không suy version chỉ từ năm.
5. SHA-256 chưa xử lý hoặc đúng với manifest của lần chạy lại.
6. Nếu cùng filename/version nhưng SHA-256 đổi: gắn `SOURCE_VERSION_CHANGED`, dừng batch để review; nếu revision hợp lệ thì đăng ký file version mới và backfill có kiểm soát.
7. Lưu `source_last_modified`, `content_length`, `sha256`, `schema_fingerprint`, `file_version`, `downloaded_at` vào `control.etl_file`.
