# 09. Kịch bản trình bày với giảng viên

## Kịch bản 8–10 phút

### 0:00–0:45 — Mở đầu

“Nhóm em thực hiện giai đoạn khảo sát nguồn và chất lượng dữ liệu trước khi thiết kế DDS cho NYC Yellow Taxi 2009–2026. Mục tiêu là xác định dữ liệu thực tế có gì, schema thay đổi ra sao và các vấn đề nào cần xử lý trong ETL. Nhóm không giả định data dictionary hiện tại đúng cho toàn bộ lịch sử.”

### 0:45–1:30 — Ba nguồn dữ liệu

“Nguồn thứ nhất là Yellow Taxi Trip Parquet, đây là dữ liệu giao dịch. Nguồn thứ hai là Taxi Zone Lookup CSV gồm 265 LocationID và thuộc tính Borough, Zone, service zone. Nguồn thứ ba là Shapefile gồm 263 polygon, dùng cho bản đồ và gán zone cho dữ liệu cũ còn lưu tọa độ.”

### 1:30–2:25 — Crawl và file đại diện

“Nhóm crawl trang TLC chính thức, không tự tạo URL. Manifest có 216 vị trí năm-tháng, trong đó 204 link trả HTTP 200 và 12 vị trí không có anchor trên trang tại ngày kiểm tra. Nhóm không tải toàn bộ 204 file vì mục tiêu giai đoạn này là khảo sát sâu schema và dị thường. Nhóm chọn bảy mốc 2009, 2010, 2015, 2019, 2024, 2025, 2026. Tổng cộng đã quét toàn bộ 60.108.247 dòng. Riêng 2024 dùng tháng 4 vì tháng 1 đến 3 không xuất hiện trong manifest HTML.”

### 2:25–3:20 — Schema evolution

“Schema thay đổi rõ rệt. Năm 2009 dùng `vendor_name`, datetime dạng string và tọa độ Start/End Lon/Lat. Năm 2010 đổi tên/case. Từ mẫu 2015 có timestamp và PULocationID/DOLocationID, thêm improvement surcharge. Năm 2024 có `Airport_fee` viết hoa A. Từ 2025 có thêm `cbd_congestion_fee`. Vì vậy ETL phải map theo version và giữ source lineage, không union cột theo vị trí.”

### 3:20–4:10 — CSV và Shapefile

“Lookup có 265 LocationID duy nhất. Shapefile có 263 feature, CRS EPSG:2263, không có geometry null, empty hay invalid. Hai ID 264 và 265 chỉ có trong CSV: Unknown và Outside of NYC, nên không có polygon cũng hợp lý. Khi làm web map, nhóm đề xuất giữ shape gốc và tạo derivative EPSG:4326.”

### 4:10–5:00 — Spatial join

“Dữ liệu 2009–2010 chưa có LocationID. Nhóm chuyển polygon từ EPSG:2263 sang EPSG:4326, tạo Point từ longitude/latitude và dùng STRtree point-in-polygon. Code đọc batch 250 nghìn dòng để tiết kiệm RAM. Tỷ lệ match một polygon trên tổng dòng khoảng 97,6 đến 97,9%. Các điểm zero, ngoài range hoặc không thuộc polygon được gắn cờ, không tự gán zone.”

### 5:00–6:20 — Data Quality

“Nhóm kiểm tra thời gian, passenger, distance/speed, LocationID, code nghiệp vụ, các khoản tiền, duplicate và spatial. Threshold duration, distance, speed dựa vào P99 từng file và chỉ là review flag. Exact duplicate thực tế là 31 dòng dư ở 2009, 16 ở 2010, 229 ở 2015 và bằng 0 ở bốn file còn lại. Passenger null tăng mạnh ở các mẫu mới: khoảng 11,63% năm 2024, 15,54% năm 2025 và 29,21% năm 2026.”

“Nhóm cũng kiểm tổng các khoản tiền ở tolerance 0,01 và 0,05 đô. Tỷ lệ mismatch cao ở một số file mới nhưng chưa kết luận total sai, vì có record mà thành phần phí null trong khi total vẫn có thể đã bao gồm. Giá trị âm cũng có thể là reversal. Tất cả được giữ và gắn cờ hoặc quarantine candidate, không xóa raw.”

### 6:20–7:15 — Output và mapping

“Project tạo 12 output chính: manifest, file/schema profile, schema evolution, profile CSV/shape, rule registry, anomaly summary, sample Parquet và Source-to-Target Mapping. Mapping cho biết source field từng version được TRY_CAST sang canonical field/type nào và liên quan DQ rule nào.”

### 7:15–8:20 — Ảnh hưởng tới DDS

“Grain dự kiến là một dòng fact cho một bản ghi chuyến hoàn tất từ meter-on đến meter-off. Fact có trip count, passenger, distance, duration, speed và các khoản tiền. Các dimension cần có là date, time, zone, vendor, payment type và rate code. Date/time/zone là role-playing cho pickup và dropoff.”

### 8:20–9:10 — Quyết định còn mở

“Trước khi cài DDS, nhóm cần chốt threshold nghiệp vụ; passenger zero và trên 6; xử lý reversal; tolerance tiền; Unknown key; spatial boundary predicate; duplicate survivorship; domain mã theo thời gian; và doanh thu quy theo pickup hay dropoff zone.”

### 9:10–9:30 — Kết luận

“Điểm quan trọng nhất là dữ liệu không đồng nhất xuyên thời gian. Project tạo được bằng chứng thực tế và pipeline chạy lại, giúp thiết kế Staging và DDS không dựa trên giả định.”

## 15 câu hỏi có thể được hỏi

### 1. Vì sao không tải toàn bộ 204 tháng?

Vì giai đoạn này cần khảo sát sâu schema và DQ, không cần hàng chục GB. Bảy mốc bao phủ các thay đổi lớn; trước production vẫn phải chạy toàn bộ tháng.

### 2. Vì sao chọn đúng bảy năm này?

2009 là khởi đầu, 2010 có rename schema, 2015 chuyển sang LocationID/improvement fee, 2019 có congestion/type changes, 2024 là hiện đại trước CBD, 2025 thêm CBD fee, 2026 là mới nhất.

### 3. Vì sao 2024 dùng tháng 4?

Crawler không thấy anchor 2024-01 đến 03 trong HTML ngày kiểm tra; code chọn tháng sớm nhất còn lại và ghi selection reason.

### 4. CSV có 265 nhưng shape có 263 là lỗi không?

Không nhất thiết. ID 264 Unknown và 265 Outside NYC chỉ ở CSV, không có vùng polygon cụ thể.

### 5. EPSG:2263 là gì?

CRS State Plane New York Long Island, đơn vị US survey foot. Shape nguồn dùng CRS này; tọa độ longitude/latitude cần EPSG:4326 để join.

### 6. Spatial join thực hiện thế nào?

Reproject polygon sang 4326, tạo Point từ lon/lat, dùng STRtree `intersects` và `within`, rồi đếm match/unmatched/multiple/boundary theo batch.

### 7. Project đã gán LocationID cho từng trip cũ chưa?

Chưa. Code hiện chỉ tính aggregate và mẫu spatial; per-trip `derived_location_id` là bước ETL tiếp theo.

### 8. Vì sao duplicate không xóa ngay?

Không có trip ID chính thức; composite duplicate có thể là hai chuyến thật trùng thuộc tính. Chỉ quarantine sau khi có survivorship rule.

### 9. Exact duplicate khác composite duplicate?

Exact giống mọi cột source; composite chỉ giống bộ vendor/time/location/passenger/distance/total được chọn.

### 10. Vì sao total mismatch chưa chắc lỗi?

Một khoản phí có thể null ở cột itemized nhưng đã nằm trong total; còn có rounding, reversal và semantics theo vendor/version.

### 11. Null khác Unknown thế nào?

Null là thiếu giá trị; Unknown là member có mã, ví dụ LocationID 264. Hai trường hợp phải có xử lý khác trong dimension.

### 12. Grain dự kiến là gì?

Một dòng `fact_taxi_trip` cho một bản ghi chuyến Yellow Taxi hoàn tất từ pickup meter-on đến dropoff meter-off.

### 13. Pickup và dropoff dùng zone thế nào?

Hai foreign key khác nhau nhưng cùng tham chiếu `dim_zone`; đây là role-playing dimension.

### 14. P99 có phải ngưỡng lỗi không?

Không. P99 là statistical review threshold theo file. Business phải chốt hard/soft threshold trước ETL.

### 15. Kết quả khảo sát ảnh hưởng DDS ra sao?

Nó quyết định canonical schema, version transformations, dimension keys, Unknown handling, DQ flags, quarantine và bộ kiểm thử ETL.
