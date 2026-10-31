# 02. Dữ liệu được lấy như thế nào?

## 1. Crawl trang TLC

### Nguồn và code

- URL gốc: `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`.
- Hằng số: `TLC_PAGE` tại `scripts/common.py:20`.
- Script: `scripts/crawl_tlc_manifest.py`.
- Hàm chính: `main(check_links=True)` tại dòng 28–60.

Thư viện được dùng:

- `requests`: tải HTML, gửi HEAD/Range request;
- `BeautifulSoup` + parser `lxml`: đọc các thẻ `<a href>`;
- `re`: nhận diện đúng tên Yellow Taxi;
- `ThreadPoolExecutor`: kiểm tra tối đa 16 URL song song;
- `pandas`: tạo và xuất manifest CSV.

### Logic crawl thực tế

1. Tạo `requests.Session` và User-Agent (`crawl_tlc_manifest.py:29–31`).
2. Tải HTML, gọi `raise_for_status()` để dừng nếu trang lỗi.
3. Duyệt mọi `a[href]` (`:34`).
4. Chuyển relative URL thành absolute URL bằng `urljoin` (`:35`).
5. Bỏ query string rồi khớp regex `yellow_tripdata_(20\d{2})-(\d{2}).(parquet|csv)` (`:15`, `:36`).
6. Chỉ giữ 2009–2026 và index theo `(year, month)` (`:38–40`).
7. Kiểm tra link song song bằng HEAD; nếu server trả 403/405 thì GET một byte Range (`head()` dòng 18–25).
8. Tạo đủ 18 × 12 = 216 vị trí. Vị trí không có anchor được ghi `missing_from_page`, không bị bỏ khỏi manifest (`:47–55`).
9. Đánh dấu URL trùng bằng `duplicated(keep=False)` (`:57–58`).
10. Kiểm tra `zero_size` từ Content-Length và `name_valid` bằng regex toàn tên file (`:55`).

### Vì sao có 204 link và 12 vị trí không có anchor?

Manifest có 216 dòng: 204 dòng `ok`, 12 dòng `missing_from_page`. Các vị trí thiếu anchor là:

- 2023-05, 2023-07 đến 2023-11;
- 2024-01 đến 2024-03;
- 2026-10 đến 2026-12.

`missing_from_page` chỉ có nghĩa crawler không thấy anchor phù hợp trong HTML tại ngày `2026-10-30`. Nó không chứng minh object CloudFront không tồn tại. Với 2026-10 đến 12, còn có yếu tố kỳ dữ liệu tương lai/chưa công bố. Khi backfill production, cần kiểm tra riêng thay vì tự tạo URL rồi coi là chính thức.

### Ví dụ một URL

`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet`

- `d37ci6vzurychx.cloudfront.net`: CDN TLC dùng để phân phối file;
- `/trip-data/`: thư mục dữ liệu chuyến;
- `yellow_tripdata`: loại Yellow Taxi;
- `2009-01`: kỳ năm-tháng;
- `.parquet`: định dạng columnar.

Kết quả được lưu tại `outputs/profiles/01_yellow_taxi_file_manifest.csv` (`crawl_tlc_manifest.py:59`).

## 2. Tải Parquet đại diện

Script `download_representative.py` đọc manifest, dùng danh sách năm cố định tại dòng 11. Nó ưu tiên January; nếu không có January trong manifest thì chọn dòng sớm nhất của năm (`:30–34`).

| Năm-tháng | File | Dung lượng byte | Số dòng | Lý do |
|---:|---|---:|---:|---|
| 2009-01 | `yellow_tripdata_2009-01.parquet` | 469.759.995 | 14.092.413 | January available |
| 2010-01 | `yellow_tripdata_2010-01.parquet` | 515.402.953 | 14.863.778 | January available |
| 2015-01 | `yellow_tripdata_2015-01.parquet` | 175.325.767 | 12.741.035 | January available |
| 2019-01 | `yellow_tripdata_2019-01.parquet` | 110.439.634 | 7.696.617 | January available |
| 2024-04 | `yellow_tripdata_2024-04.parquet` | 59.133.625 | 3.514.289 | January không có anchor; chọn tháng sớm nhất trong manifest |
| 2025-01 | `yellow_tripdata_2025-01.parquet` | 59.158.238 | 3.475.226 | January available |
| 2026-01 | `yellow_tripdata_2026-01.parquet` | 64.165.080 | 3.724.889 | January available |

### Cơ chế tải và cache

- `download()` tại dòng 14–23 kiểm tra target đã tồn tại và `stat().st_size > 0`; nếu đúng thì log `Reuse` và không tải lại.
- File mới được tải stream theo chunk 8 MiB vào file `.part`, sau đó `replace()` thành tên chính thức. Raw hoàn chỉnh không bị ghi từng phần.
- Timeout là 30 giây kết nối và 300 giây đọc.
- HTTP lỗi gọi `raise_for_status()` nên script dừng; `.part` có thể còn lại và lần chạy sau tải lại từ đầu.
- Sau khi tải/reuse, `sha256()` trong `common.py:41–46` đọc file theo chunk 1 MiB và ghi checksum vào `representative_files.csv` (`download_representative.py:37–39`).

Raw được lưu trong `data/raw/`. Project không có script tự đối chiếu checksum hiện tại với một checksum chuẩn cũ; checksum là bằng chứng để người vận hành so sánh hoặc dùng trong test bổ sung.

### Trường hợp 2024

Code không hard-code “tháng 4”. Nó thấy January không có trong manifest, lấy candidate sớm nhất bằng `cand.iloc[0]` và ghi `selection_reason = January unavailable; nearest available month selected`. Trong manifest hiện tại, candidate đầu tiên là 2024-04.

## 3. Dữ liệu tham chiếu

| Nguồn | Trạng thái trong project | Cách đưa vào |
|---|---|---|
| `taxi+_zone_lookup.csv` | Có trong `data/reference` | File được cung cấp sẵn; README yêu cầu người chạy đặt nguyên trạng vào đây |
| `taxi_zones.zip` | Có trong `data/reference` | File được cung cấp sẵn; giữ ZIP gốc |
| Shapefile components | Có dưới `data/reference/taxi_zones_extracted/taxi_zones/` | `profile_shapefile.py:15–16` tự giải nén nếu chưa tìm thấy `.shp` |
| Đề bài PDF | `cq_BI_PROJECT_LAB_2627.pdf` | Được cung cấp sẵn và sao chép vào reference |
| Text đề bài | `project_brief_extracted.txt` | Artifact hỗ trợ đọc PDF; không có script project tạo lại |
| Yellow Trips Data Dictionary | Không có bản PDF local | Báo cáo tham chiếu URL TLC; code không tải/lưu dictionary |

README chỉ yêu cầu ba input local: lookup CSV, zone ZIP và project brief PDF tùy chọn (`README.md:14–22`). Yellow dictionary được dùng ở mức tài liệu tham khảo, không phải input trực tiếp của các script.

## 4. File kiểm soát việc tải

`outputs/profiles/representative_files.csv` là inventory thực tế của 7 file đã chọn. Các cột nên kiểm tra:

- `year`, `month`, `file_name`, `url`;
- `selection_reason`;
- `local_path`;
- `downloaded_size_bytes`;
- `sha256`;
- `access_status` và `http_status`.

Đây là cầu nối truy vết từ URL chính thức đến file local, profile và kết quả DQ.
