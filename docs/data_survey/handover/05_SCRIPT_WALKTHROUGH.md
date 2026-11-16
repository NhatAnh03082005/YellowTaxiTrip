# 05. Giải thích 11 file Python

## 1. Bảng tổng quan

| Thứ tự | Script | Mục đích | Input chính | Output | Chạy độc lập? |
|---:|---|---|---|---|---|
| 0 | `common.py` | Path, logger, SHA-256 helper | — | thư mục/log/hash | Không; module dùng chung |
| 1 | `crawl_tlc_manifest.py` | Crawl danh mục TLC | TLC HTML | `01` | Có, cần mạng |
| 2 | `download_representative.py` | Chọn và tải 7 file | `01` | raw + `representative_files.csv` | Sau `01`, cần mạng |
| 3 | `profile_parquet.py` | Profile footer/schema | raw + representative list | `02`, `03` | Sau tải |
| 4 | `profile_taxi_zone_csv.py` | Profile lookup | Zone CSV | `05`, `06` | Có nếu CSV sẵn |
| 5 | `profile_shapefile.py` | Giải nén/profile/compare | ZIP + CSV | `07`, `08` | Có nếu reference sẵn |
| 6 | `run_data_quality_checks.py` | Core DQ toàn file | raw + representative + CSV | `10`, `11`, percentile | Sau 2; tốn thời gian |
| 7 | `profile_legacy_spatial.py` | Spatial DQ 2009/2010 | core DQ + shape + raw | spatial summary; cập nhật `10`,`11` | Sau 5 và 6 |
| 8 | `recompute_duplicate_counts.py` | Tính lại duplicate chính xác | `10` + raw | cập nhật `10` | Sau 6; hiện mang tính repair/verify |
| 9 | `enrich_temporal_fee_checks.py` | Rule fee theo zone/ngày | `10`,`11` + raw | cập nhật `10`,`11` | Sau 6/7 |
| 10 | `generate_deliverables.py` | Tạo evolution, registry, STM, report | `03`, `10` và profile | `04`,`09`,`12`, report | Sau toàn bộ phân tích |

Lệnh dưới đây giả định đang đứng tại `D:\BI\DATH` và đã có `.venv`.

## 2. `common.py`

- **Mục đích:** Định nghĩa project-relative paths, tạo thư mục, cấu hình log và tính SHA-256.
- **Input:** Không đọc dữ liệu khi import.
- **Hàm chính:** `init_dirs()` dòng 23–25; `logger()` dòng 28–38; `sha256()` dòng 41–46; `write_json()` dòng 49–50 (hiện không được script khác gọi).
- **Luồng:** resolve root từ vị trí file → khai báo path → tạo thư mục nếu cần → tạo FileHandler/StreamHandler → hash theo chunk 1 MiB.
- **Output:** thư mục, log file và giá trị hash trả về.
- **Phụ thuộc:** Mọi script khác import module này.
- **Thư viện:** `pathlib`, `logging`, `hashlib`, `json`.
- **Cách chạy:** Không cần chạy trực tiếp; `python common.py` không làm gì ngoài định nghĩa.
- **Thành công:** Script gọi nó tạo đúng `logs/<name>.log`.
- **Lỗi thường gặp:** Không có quyền ghi project root.
- **Liên hệ đồ án:** Bảo đảm không hard-code đường dẫn máy và có raw lineage.

## 3. `crawl_tlc_manifest.py`

- **Mục đích:** Lập inventory đủ 216 vị trí năm-tháng và kiểm tra link.
- **Input:** TLC page từ `common.TLC_PAGE`.
- **Hàm:** `head()` dòng 18–25; `main()` dòng 28–60.
- **Luồng:** tải HTML → parse link → regex Yellow → giữ 2009–2026 → HEAD song song → tạo missing slots → duplicate/name/zero checks → CSV.
- **Output:** `outputs/profiles/01_yellow_taxi_file_manifest.csv`, log.
- **Phụ thuộc:** Mạng; không cần file local.
- **Thư viện:** requests, BeautifulSoup/lxml, regex, pandas, concurrent futures.
- **Cách chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\crawl_tlc_manifest.py`.
- **Thành công:** log ghi `204 linked months, 12 missing page slots`; CSV 216 dòng.
- **Lỗi:** DNS/SSL/timeout; page đổi HTML; HEAD bị chặn. Có `--no-head` để chỉ parse link nhưng status thành `not_checked`.
- **Liên hệ:** Source inventory và `DQ-FILE`.

## 4. `download_representative.py`

- **Mục đích:** Chọn bảy mốc và tải raw có cache/checksum.
- **Input:** `01`.
- **Hàm:** `download()` dòng 14–23; `main()` dòng 26–39.
- **Luồng:** lọc year → ưu tiên Jan → chọn earliest fallback → reuse non-zero target → stream vào `.part` → rename → SHA-256 → inventory.
- **Output:** 7 `data/raw/*.parquet`, `representative_files.csv`, log.
- **Phụ thuộc:** `01`, mạng nếu file chưa có.
- **Thư viện:** requests, pandas, pathlib, hashlib qua common.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\download_representative.py`.
- **Thành công:** 7 dòng representative và 7 raw file non-zero.
- **Lỗi:** year không có candidate gây `iloc[0]`; network làm còn `.part`; không resume byte-range.
- **Liên hệ:** Tạo sample thời gian cho schema/DQ.

## 5. `profile_parquet.py`

- **Mục đích:** Đọc metadata và schema thực tế, không nạp toàn bộ file.
- **Input:** representative list + 7 raw files.
- **Hàm:** `main()` dòng 11–33; helper `safe()` nội bộ dòng 26–29.
- **Luồng:** mở ParquetFile → footer/schema → file metrics → duyệt column × row group → statistics → CSV.
- **Output:** `02`, `03`, log.
- **Phụ thuộc:** Download hoàn tất.
- **Thư viện:** PyArrow Parquet, pandas, time.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_parquet.py`.
- **Thành công:** `02` có 7 dòng; `03` có 133 dòng; log nêu rows/cols.
- **Lỗi:** corrupt footer, thiếu raw, metadata statistics không có. Null count ở đây phụ thuộc writer metadata.
- **Liên hệ:** Schema evolution, canonical schema, capacity planning.

## 6. `profile_taxi_zone_csv.py`

- **Mục đích:** Profile toàn bộ lookup và liệt kê issue/special member.
- **Input:** `data/reference/taxi+_zone_lookup.csv`.
- **Hàm:** `main()` dòng 8–34; helper lồng `add()` dòng 18–19.
- **Luồng:** đọc CSV → per-column metrics → borough/service distributions → duplicate ID/name → whitespace → ID gap → special text → explicit consistency rules → CSV.
- **Output:** `05`, `06`, log.
- **Phụ thuộc:** Chỉ cần lookup.
- **Thư viện:** pandas.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_taxi_zone_csv.py`.
- **Thành công:** log `rows=265 cols=4 issues=27`.
- **Lỗi:** đổi tên cột/case sẽ gây KeyError; rule Manhattan/EWR là rule explicit trong code, không phải ontology tổng quát.
- **Liên hệ:** `dim_zone`, Unknown/Outside handling.

## 7. `profile_shapefile.py`

- **Mục đích:** Giải nén shape nếu cần, profile CRS/geometry và compare với CSV.
- **Input:** zone ZIP, lookup CSV; shape extracted nếu đã có.
- **Hàm:** `main()` dòng 12–40.
- **Luồng:** detect `.shp` → unzip nếu thiếu → GeoPandas read → components → CRS/bounds/validity/duplicates → output profile → outer join LocationID → borough/zone mismatch.
- **Output:** `07`, `08`, extracted shape, log.
- **Phụ thuộc:** Reference ZIP + CSV.
- **Thư viện:** zipfile, GeoPandas, pandas, Shapely qua GeoPandas.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_shapefile.py`.
- **Thành công:** 263 features, 0 invalid, 265 comparison rows.
- **Lỗi:** thiếu `.dbf/.shx`, CRS null, column names không có LocationID/borough/zone.
- **Liên hệ:** Spatial dimension/map.

## 8. `run_data_quality_checks.py`

- **Mục đích:** Canonicalize schema từng version rồi chạy core DQ trên toàn bộ 7 file.
- **Input:** representative list, raw Parquet, lookup CSV.
- **Hàm:** `qi()`/`val()` dòng 28–30; `base_sql()` dòng 33–43; `main()` dòng 46–88.
- **Luồng:** map source fields theo `CANON` → TRY_CAST → derive duration/speed/expected total → percentile → conditional counts → lấy tối đa 3 samples → exact/composite duplicate → write summary/sample/stats.
- **Output:** `10`, `11`, `distribution_percentiles.csv`, log. Nó **ghi đè** `10` và `11`.
- **Phụ thuộc:** 7 raw + representative + lookup.
- **Thư viện:** DuckDB, pandas, PyArrow, JSON.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\run_data_quality_checks.py`.
- **Thành công:** log đủ 7 dòng `DQ complete`; output có 7 files.
- **Lỗi:** nhiều I/O/CPU; schema mới chưa có trong `CANON`; file khóa; DuckDB memory. P99 là statistical flag.
- **Liên hệ:** Rule catalog, ETL quarantine/test.

## 9. `profile_legacy_spatial.py`

- **Mục đích:** Kiểm tra tọa độ và point-in-polygon cho 2009/2010.
- **Input:** raw 2009/2010, extracted shape, representative list, `10`,`11` core.
- **Hàm:** `scan_side()` dòng 17–31; `main()` dòng 34–49.
- **Luồng:** reproject shape 4326 → STRtree → đọc batch 250k → null/zero/range/swapped → points → intersects/within → matched/unmatched/multiple/boundary → append rule/sample.
- **Output:** `legacy_spatial_summary.csv`; cập nhật `10`, `11`.
- **Phụ thuộc:** Profile shape và core DQ phải chạy trước.
- **Thư viện:** NumPy, PyArrow, GeoPandas, Shapely STRtree.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_legacy_spatial.py`.
- **Thành công:** log bốn tổ hợp 2009/2010 × pickup/dropoff.
- **Lỗi:** rất tốn CPU; CRS sai; rerun sẽ append duplicate spatial rows/samples vì code không xóa cũ.
- **Liên hệ:** Suy ra zone cho legacy, nhưng code hiện chỉ xuất aggregate/mẫu, không xuất per-trip LocationID.

## 10. `recompute_duplicate_counts.py`

- **Mục đích:** Tính lại exact/composite duplicate và cập nhật các dòng DQ-DUP.
- **Input:** raw, representative list, `10`.
- **Hàm:** Không có `main()`; code chạy ở module top-level dòng 6–18.
- **Luồng:** mở DuckDB → từng file → `GROUP BY ALL` full row → group composite canonical → update failed count/rate → loại hai rule lịch sử 2009/2010 → ghi lại `10`.
- **Output:** cập nhật `10`, log.
- **Phụ thuộc:** Core DQ đã tạo `10`.
- **Thư viện:** DuckDB, pandas.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\recompute_duplicate_counts.py`.
- **Thành công:** log full/composite cho 7 file.
- **Lỗi:** group toàn file tốn tài nguyên; nếu DQ-DUP row không có thì không thêm mới.
- **Liên hệ:** Duplicate evidence. Logic hiện trùng với core DQ đã sửa, nên script chủ yếu là repair/verification lịch sử.

## 11. `enrich_temporal_fee_checks.py`

- **Mục đích:** Bổ sung rule airport fee, CBD fee và surcharge trước hiệu lực.
- **Input:** representative, raw, `10`, `11`.
- **Hàm:** Không có `main()`; top-level dòng 9–21, dùng `base_sql()` từ core.
- **Luồng:** chọn rule theo schema year → DuckDB count → replace same rule/file trong summary → lấy 3 samples → xóa sample fee cũ → append sample mới.
- **Output:** cập nhật `10`, `11`, log.
- **Phụ thuộc:** Core DQ; spatial có thể trước hoặc sau nhưng README đặt trước.
- **Thư viện:** DuckDB, pandas, PyArrow, JSON.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\enrich_temporal_fee_checks.py`.
- **Thành công:** log rule 008–010 theo file áp dụng.
- **Lỗi:** airport IDs 132/138 và effective dates nằm trực tiếp trong code; cần governance khi chính sách đổi.
- **Liên hệ:** Fee DQ và temporal business rules. Core DQ hiện cũng đã có các rule này, nên enrichment là bước replace/repair.

## 12. `generate_deliverables.py`

- **Mục đích:** Sinh các bảng tổng hợp cuối và report.
- **Input:** `03`, `10`, các profile/anomaly phụ.
- **Hàm:** `schema_evolution()` 16–33; `mapping()` 36–48; `dq_rules()` 51–61; `md_table()` 64–65; `report()` 68–210; `main()` 213–216.
- **Luồng:** canonical comparison → STM → aggregate DQ registry → đọc profile → tính headline → render Markdown.
- **Output:** `04`, `09`, `12`, final report, log.
- **Phụ thuộc:** Tất cả profile/DQ cần có nếu muốn report đầy đủ.
- **Thư viện:** pandas; import `CANON` từ core DQ.
- **Chạy:** `.venv\Scripts\python.exe nyc_taxi_survey\scripts\generate_deliverables.py`.
- **Thành công:** log `Generated schema evolution, DQ registry, mapping and report`.
- **Lỗi:** thiếu bất kỳ CSV bắt buộc; `to_markdown` cần `tabulate`; report có thể phản ánh output cũ nếu upstream chưa rerun.
- **Liên hệ:** Deliverable để nộp và nền tảng DDS/STM.
