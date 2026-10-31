# 07. Data Quality được kiểm tra như thế nào?

## 1. Nguyên tắc chung

Core DQ không sửa raw. `base_sql()` tạo một lớp canonical tạm bằng `TRY_CAST`, tính `duration_minutes`, `average_speed_mph` và `expected_total` (`run_data_quality_checks.py:33–43`). Mỗi rule trả về `checked_rows`, `failed_rows`, rate, severity và recommendation. Mẫu lỗi được lưu riêng, không xuất toàn bộ dòng vi phạm.

## 2. Các nhóm kiểm tra

| Câu hỏi kiểm tra | Vì sao | Cách code kiểm tra | Kết quả | Tự xóa? |
|---|---|---|---|---|
| File có link/đọc được? | Không thể ETL nếu source hỏng | HTTP 200/206; PyArrow footer | `01`, `02`, `DQ-FILE-001` | Không; reject/retry file |
| Schema có đúng version? | Tránh đọc nhầm tên/type | `CANON`, actual schema, evolution | `03`, `04`, `DQ-SCHEMA-001` | Không; version mapping |
| Datetime null/âm/zero/sai tháng? | Sai duration và time dimensions | `dropoff < pickup`, equality, year/month | `10`, rule TIME | Không; flag/quarantine |
| Duration quá lớn? | Phát hiện outlier | `duration > P99` từng file | percentile + `DQ-TIME-005` | Không; P99 là review flag |
| Passenger null/âm/zero/>6? | Ảnh hưởng passenger KPI/group | numeric conditions dòng 58 | `DQ-PASSENGER-*` | Không; >6 cần business confirmation |
| Distance null/âm/zero/lớn? | Ảnh hưởng distance/speed | conditions và file P99 | `DQ-DISTANCE-*` | Không |
| Speed bất thường? | Distance/time mâu thuẫn | distance ÷ duration hours; >P99 | percentile, `DQ-DISTANCE-005` | Không |
| LocationID null/non-integer/unmatched? | Zone dimensions/map sai | membership trực tiếp trong 265 IDs | `DQ-LOCATION-*` | Không; có thể map Unknown theo policy |
| Vendor/rate/payment/store hợp lệ? | Dimension code và KPI | domain theo năm, không backcast 2009/10 | `DQ-CODE-*` | Không |
| Khoản tiền null/âm/zero? | Revenue risk | fare/fees/total conditions | `DQ-AMOUNT-001..005` | Không; âm có thể reversal |
| Tổng thành phần khớp total? | Reconciliation | abs(expected-total)>0.01/0.05 | `DQ-AMOUNT-006/007` | Không |
| Airport/CBD/surcharge đúng zone/ngày? | Business rule | IDs 132/138; effective timestamps | `DQ-AMOUNT-008..010` | Không; cần điều tra |
| Full row trùng? | Có thể nạp fact hai lần | `GROUP BY ALL`, sum(count-1) | `DQ-DUP-001` | Không; quarantine candidate |
| Composite key trùng? | Không có trip ID chính thức | group canonical business fields | `DQ-DUP-002` | Không |
| Tọa độ invalid/unmatched? | Không suy ra được zone cũ | range checks + STRtree polygon query | spatial summary, `DQ-SPATIAL-*` | Không |

## 3. Time và sai tháng

Các rule TIME nằm ở `run_data_quality_checks.py:57`:

- `TIME-001`: null hoặc string không cast được timestamp;
- `TIME-002`: dropoff trước pickup;
- `TIME-003`: pickup bằng dropoff;
- `TIME-004`: pickup không thuộc year-month trong tên file;
- `TIME-005`: duration trên P99 của chính file.

Dropoff sang đầu tháng tiếp theo không bị `TIME-004` vì condition chỉ kiểm pickup. Đây là chủ ý hợp lý cho chuyến qua nửa đêm. Code chưa xuất histogram độ lệch tháng chi tiết; nó chỉ count và sample.

## 4. Passenger và distance

Passenger được canonical về DOUBLE để cho phép phát hiện non-integer giữa các schema. Rule >6 là threshold nghiệp vụ cần duyệt, không phải lỗi chắc chắn.

Speed chỉ tính khi `dropoff > pickup` và `distance >= 0` (`base_sql()` dòng 43). P50/P90/P95/P99/max được lưu ở `distribution_percentiles.csv`. Cả distance và speed dùng P99 từng file; khoảng 1% bị gắn cờ theo định nghĩa thống kê.

## 5. LocationID, Null và Unknown

- **Null:** không có giá trị hoặc cast thất bại.
- **Unknown:** một giá trị danh mục có chủ ý, ví dụ LocationID 264.
- **Outside NYC:** LocationID 265, cũng là member lookup nhưng không có polygon.

Code không dùng `min <= ID <= max` để kết luận hợp lệ. Nó tạo danh sách toàn bộ 265 IDs từ CSV và kiểm `NOT IN` (`run_data_quality_checks.py:48,63`). ID 264/265 được rule riêng `LOCATION-004`, không bị coi là unmatched lookup.

## 6. Mã nghiệp vụ theo version

2009/2010 có code dạng chuỗi và không bị áp domain hiện hành. 2015/2019 dùng vendor 1/2, payment 1–6. Từ 2024 vendor domain 1/2/6/7; 2025+ payment cho phép thêm 0 (`run_data_quality_checks.py:64–67`). `rate IS NULL` được kiểm, nhưng code chưa có rule out-of-domain RatecodeID riêng.

## 7. Tiền và lý do không xóa tự động

`expected_total` là tổng `fare, extra, mta, tip, tolls, improvement, congestion, airport, cbd`, với NULL được coalesce 0. Những lý do mismatch chưa chắc là lỗi:

- một thành phần có thể null nhưng total đã bao gồm;
- schema cũ không có một phí;
- giao dịch reversal/adjustment có giá trị âm;
- tip cash không có trong `tip_amount`;
- rounding hoặc semantics vendor khác.

Airport fee mismatch cũng chưa chắc sai tuyệt đối: rule hiện chỉ nhìn pickup ID khác 132/138; timestamp/location có thể lỗi, hoặc fee semantics cần xác nhận. Kết quả phải quarantine/điều tra, không xóa.

## 8. Exact duplicate và composite duplicate

### Exact duplicate

Hai dòng giống mọi source column. Code chọn `*`, group toàn bộ và cộng `count-1` (`run_data_quality_checks.py:82`). Kết quả hiện tại: 31 dòng dư ở 2009, 16 ở 2010, 229 ở 2015, 0 ở bốn file còn lại.

### Composite duplicate

Hai dòng giống vendor, pickup/dropoff, pickup/dropoff zone hoặc tọa độ, passenger, distance và total (`:83`). Nó là candidate vì hai chuyến thật có thể cùng bộ thuộc tính. Kết quả hiện tại gần exact duplicate, nhưng không được dùng như primary key.

Code không tạo `record_hash`; hash chỉ là đề xuất trong report/kiến trúc tương lai.

## 9. Spatial quality

`profile_legacy_spatial.scan_side()` kiểm:

1. null/non-finite;
2. cả longitude và latitude bằng 0;
3. ngoài range WGS84;
4. dấu hiệu đảo lon/lat;
5. không intersect polygon;
6. match nhiều polygon;
7. intersects nhưng không within (boundary candidate).

Nguyên nhân unmatched có thể là điểm ngoài NYC, sai tọa độ, GPS/meter lỗi, zone không phủ toàn bộ khu vực, điểm tại vùng nước/biên, hoặc giả định CRS sai. Shape được reproject sang 4326; không ghi đè shape gốc.

## 10. Schema evolution khác anomaly

- **Schema evolution:** đổi hợp lệ theo thời gian, ví dụ `vendor_name` → `VendorID`, tọa độ → LocationID, thêm CBD fee. Xử lý bằng version mapping và severity Info.
- **Data anomaly:** giá trị trong một schema cụ thể vi phạm rule, ví dụ dropoff trước pickup hoặc LocationID không có trong lookup.

Không được coi “cột không tồn tại ở 2009” là null anomaly của từng dòng; đó là structural absence.

## 11. Vì sao cần thống nhất threshold trước ETL?

P99 thay đổi theo file; passenger >6 chưa chắc sai; speed cao có thể do duration cực nhỏ; total mismatch phụ thuộc semantics phí. Nếu ETL tự reject bằng ngưỡng chưa duyệt, nhóm có thể làm mất doanh thu/chuyến hợp lệ. Trước DDS cần chốt:

- hard reject hay soft flag;
- threshold theo global hay theo period;
- Unknown surrogate key;
- duplicate survivorship;
- negative transaction policy;
- money tolerance;
- spatial predicate và boundary handling.
