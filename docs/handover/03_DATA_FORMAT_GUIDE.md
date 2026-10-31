# 03. Hướng dẫn các định dạng dữ liệu

## 1. Parquet

Parquet là định dạng nhị phân dạng cột. “Columnar storage” nghĩa là giá trị của cùng một cột được đặt gần nhau, thay vì lưu lần lượt toàn bộ trường của từng dòng như CSV. Cách này giúp nén tốt và chỉ đọc những cột cần thiết.

Một file Parquet chứa:

- **schema:** tên cột, thứ tự, logical type và nullability;
- **physical type:** dạng lưu dưới thấp như `INT32`, `DOUBLE`, `BYTE_ARRAY`;
- **row group:** một khối dòng có metadata riêng, cho phép đọc theo khối;
- **statistics:** nếu writer cung cấp, có null count, min, max theo column chunk;
- **footer:** metadata tổng hợp ở cuối file.

Vì footer có `num_rows`, `num_columns`, row groups và schema, `profile_parquet.py` dùng `pq.ParquetFile()` để đọc profile mà chưa nạp 60 triệu dòng vào RAM. Với từng column/row group, code đọc statistics tại `profile_parquet.py:18–30`.

Project dùng Parquet theo hai vai trò khác nhau:

1. **Raw Yellow Taxi Parquet** trong `data/raw/`: dữ liệu giao dịch chính thức, hàng triệu dòng/file; không bị sửa.
2. **`11_anomaly_samples.parquet`** trong `outputs/anomalies/`: chỉ 404 mẫu lỗi có schema `file, year, month, rule_id, sample_json`; đây là output nhỏ để xem ví dụ, không phải raw trip data.

Công cụ đọc Parquet:

- PyArrow: footer/schema/batch và ghi sample;
- DuckDB: SQL quét trực tiếp file, percentile và DQ;
- pandas: đọc các bảng điều khiển CSV, không nạp raw Parquet chính.

## 2. CSV

CSV là text có các cột phân tách bằng dấu phẩy. Nó dễ mở, dễ kiểm tra và phù hợp với bảng danh mục hoặc bảng kết quả vừa/nhỏ.

`taxi+_zone_lookup.csv` có bốn cột:

- `LocationID`: khóa tự nhiên dùng để join;
- `Borough`: quận/khu vực cấp cao;
- `Zone`: tên taxi zone;
- `service_zone`: nhóm dịch vụ như Yellow Zone, Boro Zone, Airports.

CSV phù hợp làm reference vì chỉ có 265 dòng, được dùng thường xuyên để map mã sang mô tả và xây `dim_zone`.

### Các CSV output

| Số | File | Mục đích ngắn |
|---:|---|---|
| 01 | Manifest | Danh sách 216 kỳ, URL và trạng thái |
| 02 | Parquet file profile | Một dòng/file: size, rows, columns, hash |
| 03 | Schema by file | Một dòng/cột/file |
| 04 | Schema evolution | So canonical field qua 7 phiên bản |
| 05 | Zone lookup profile | Null, distinct và distribution |
| 06 | Zone lookup issues | Các dòng duplicate-name/special-case |
| 07 | Shapefile profile | CRS, component, geometry metrics |
| 08 | CSV–shape comparison | Join 265 LocationID giữa hai nguồn |
| 09 | DQ rules | Registry rule tổng hợp |
| 10 | Anomaly summary | Kết quả rule theo file |
| 12 | Source-to-Target Mapping | Mapping source → canonical/Staging/target |

Ngoài bộ số còn có `representative_files.csv`, `distribution_percentiles.csv` và `legacy_spatial_summary.csv`.

## 3. Shapefile ZIP

Shapefile là một bộ nhiều file cùng basename, không phải một file `.shp` độc lập:

- `.shp`: geometry của feature;
- `.shx`: index vị trí geometry để truy cập nhanh;
- `.dbf`: bảng thuộc tính như LocationID, zone, borough;
- `.prj`: mô tả hệ tọa độ;
- `.cpg`: encoding ký tự của DBF.

ZIP nguồn có đủ năm thành phần này. Code coi `.shp`, `.shx`, `.dbf` là bắt buộc tối thiểu (`profile_shapefile.py:21,25`) và vẫn liệt kê `.prj`, `.cpg` trong profile.

### Polygon và CRS

- **Polygon/MultiPolygon:** vùng khép kín biểu diễn ranh giới taxi zone; một zone có thể gồm một hoặc nhiều phần.
- **CRS:** quy tắc biến cặp số tọa độ thành vị trí trên Trái Đất.
- **EPSG:2263:** New York Long Island State Plane, đơn vị US survey foot. Shape nguồn dùng CRS này, thích hợp phân tích địa phương nhưng không dùng trực tiếp với longitude/latitude WGS84.

Khi spatial join, code chuyển polygon sang EPSG:4326 (`profile_legacy_spatial.py:36`) vì điểm legacy được xem là longitude/latitude WGS84. Web map thường cũng cần EPSG:4326. Phải giữ nguyên shape nguồn và chỉ tạo representation chuyển CRS trong bộ nhớ hoặc derivative riêng.

Không nên chỉ copy `.shp`: thiếu `.dbf` sẽ mất LocationID/Zone; thiếu `.prj` có thể không biết CRS; thiếu `.shx` ảnh hưởng khả năng đọc/index.

## 4. Markdown, log, checksum và môi trường

### Markdown

`NYC_Yellow_Taxi_Source_Survey_and_Data_Quality_Report.md` là báo cáo phân tích có bảng và Mermaid. README là hướng dẫn cài/chạy ngắn; báo cáo cuối giải thích kết quả và đề xuất mô hình. Handover hiện tại giải thích cách project hoạt động.

### Log

Mỗi executable script lấy logger từ `common.py:28–38`. Log được append vào `logs/<script>.log` đồng thời in ra console. Dùng log để biết file nào đã reuse/tải, số dòng profile, số rule spatial và thời điểm hoàn thành. Log không ghi stack trace lỗi nếu exception xảy ra trước câu `log.info`; cần xem cả console/exit code.

### SHA-256 checksum

SHA-256 tạo “dấu vân tay” 64 ký tự cho nội dung raw file. Nếu một byte thay đổi, checksum gần như chắc chắn thay đổi. Hàm `sha256()` ở `common.py:41–46`; checksum hiện nằm trong `representative_files.csv` và `02_parquet_file_profile.csv`.

### `requirements.txt`

Liệt kê thư viện và version tối thiểu: pandas/numpy, PyArrow/DuckDB, GeoPandas/Shapely/Pyogrio, requests/BeautifulSoup/lxml, pypdf và tabulate. Nó giúp tạo lại môi trường thay vì cài thủ công theo trí nhớ.

### README khác báo cáo cuối

- README: input cần đặt ở đâu, lệnh chạy, output nằm ở đâu.
- Báo cáo: số liệu, schema evolution, anomalies, grain/fact/dimension và hạn chế.
- Handover: giải thích quan hệ giữa code, input, output và cách thuyết trình/chạy lại.
