# Phase 2 Design Freeze — đã phê duyệt

**Phạm vi:** NYC Yellow Taxi, ETL 2024–2025  
**Ngày lập:** 2026-11-15 (Asia/Bangkok)  
**Cập nhật phê duyệt:** 2026-11-16 (Asia/Bangkok)  
**Trạng thái:** `APPROVED` — Project Lead và dbt Owner đã ký duyệt.  
**Reviewer:** Data Quality/Reconciliation review và Data Model/Power BI review đã hoàn tất.

## 1. Decision Log

| ID | Quyết định chốt đề xuất | Lý do thực tế | Owner | Trạng thái |
|---|---|---|---|---|
| DF-01 | ETL đủ 24 tháng của 2024 và 2025 | Có dữ liệu trước/sau khi xuất hiện `cbd_congestion_fee`; đủ May/June và đủ YoY | Lead | APPROVED |
| DF-02 | Revenue mặc định theo **Pickup Zone** | Phù hợp phân tích nơi phát sinh nhu cầu/doanh thu; airport fee cũng theo pickup | Lead + BI | APPROVED |
| DF-03 | `revenue_amount = total_amount` cho accepted positive trip | TLC định nghĩa đây là tổng tiền khách bị tính; component sum lệch 30–32% trong hai file profile | Lead + dbt | APPROVED |
| DF-04 | Hard reject chỉ cho lỗi kỹ thuật; business quarantine giữ nguyên dòng và lineage | Không được mất dữ liệu nguồn; rejected row không vào trusted Fact | Lead + dbt | APPROVED |
| DF-05 | Idempotency dùng source identity; chỉ auto-quarantine raw-exact duplicate trong cùng file; cross-file duplicate chỉ warning | Dataset không có trip ID chính thức; fingerprint không phải khóa định danh chuyến | Lead + dbt | APPROVED |
| DF-06 | Surrogate `0 = Unknown`, `-1 = Not Applicable`; FK Fact không null | Tách thiếu/không map khỏi không áp dụng; sequence bình thường bắt đầu từ 1 | Lead + dbt | APPROVED |
| DF-07 | Zone 264 và 265 là member nguồn hợp lệ, có surrogate key dương | 264 = Unknown, 265 = Outside NYC; không đồng nhất với technical Unknown key 0 | Lead + dbt | APPROVED |
| DF-08 | Zone, Vendor, Payment, Rate đều **SCD Type 1** trong phạm vi này | Chỉ có một snapshot reference/dictionary; chưa có bằng chứng cùng code đổi nghĩa theo thời gian | Lead + dbt | APPROVED |
| DF-09 | Recorded credit-card tip rate = `SUM(tip_amount) / SUM(fare_amount)` trên tập hợp lệ | Phù hợp định hướng mô hình hóa; cash tip không được ghi đầy đủ | Lead + BI | APPROVED |
| DF-10 | Speed chỉ tính cho distance > 0 và duration > 0; 45–80 mph warning, >80 mph quarantine | P99 thực tế chỉ khoảng 35 mph nhưng P99 không phải ranh giới vật lý | Lead + dbt | APPROVED |
| DF-11 | Tách row-count reconciliation khỏi amount-eligible reconciliation; component tolerance $0.05 chỉ là warning | Dòng technical reject có thể không cast được amount nên không thể ép cân tổng tiền | Lead + dbt | APPROVED |
| DF-12 | Một dimension vật lý; sáu role views/tables trong Power BI cho Pickup/Dropoff Date/Time/Zone | Cho phép tất cả relationship active, tránh phụ thuộc `USERELATIONSHIP` | Lead + BI | APPROVED |

## 2. Phạm vi file chính thức

- Hai năm: `2024`, `2025`.
- 24 file: `yellow_tripdata_YYYY-MM.parquet`, tháng 01–12 cho mỗi năm.
- Kiểm tra HTTP HEAD ngày 2026-11-15: cả 24 URL trực tiếp trả HTTP 200, gồm 2024-01..03 từng bị manifest cũ ghi `missing_from_page`.
- May/June có đủ bốn file: 2024-05, 2024-06, 2025-05, 2025-06.
- Trước khi load thật, ingestion vẫn phải download, kiểm tra Parquet footer, SHA-256 và lưu content length. HTTP 200 không thay thế checksum.
- Chi tiết URL nằm tại `docs/mapping/INPUT_FILES_2024_2025.md`.

## 3. Định nghĩa KPI đã chốt đề xuất

### 3.1 Revenue

```sql
reported_total_amount = cast(total_amount as numeric(12,2))

revenue_amount = case
  when primary_disposition in ('ACCEPTED', 'WARNING')
   and reported_total_amount > 0
  then reported_total_amount
end
```

- Revenue dashboard mặc định: **Gross Accepted Revenue** = `SUM(revenue_amount)` với `is_revenue_kpi_eligible = true`, theo Pickup Date/Pickup Zone.
- Không thay `total_amount` bằng tổng component.
- Dropoff revenue vẫn cho phép phân tích, nhưng visual phải ghi rõ “Revenue by Dropoff Zone”.
- Dòng âm/0, void/reversal/adjustment đi quarantine; không cộng vào gross revenue. Có thể tạo KPI audit `Net Reported Amount` riêng trên staging/quarantine, không dùng làm KPI vận hành mặc định.

### 3.2 Tip rate

```sql
tip_rate = tip_amount / nullif(fare_amount, 0)
```

Tập chuyến hợp lệ:

- `payment_type_id = 1` (Credit card);
- accepted hoặc warning, không phải reversal/void;
- `fare_amount > 0`, `tip_amount >= 0`.

KPI aggregate phải là:

```sql
sum(tip_amount) / nullif(sum(fare_amount), 0)
```

Không lấy trung bình các `tip_rate` từng dòng. Tên KPI: **Recorded credit-card tip rate**; Power BI lọc `is_tip_kpi_eligible = true`.

### 3.3 Duration và speed

```sql
duration_seconds = extract(epoch from (dropoff_datetime - pickup_datetime))::integer

row_speed_mph = case
  when duration_seconds > 0 and trip_distance > 0
  then round(trip_distance * 3600 / duration_seconds, 2)
end
```

| Điều kiện | Disposition | Dùng trong KPI speed mặc định |
|---|---|---|
| 0 < speed <= 45 mph | Accepted | Có |
| 45 < speed <= 80 mph | Warning | Không |
| speed > 80 mph | Business quarantine | Không |
| duration <= 0 | Business quarantine | Không |
| 180 phút < duration <= 24 giờ | Warning | Không |
| duration > 24 giờ | Business quarantine | Không |

P99 theo file chỉ là monitoring statistic. Không dùng `distance > P99`, `duration > P99` hoặc `speed > P99` làm reject.

KPI speed tổng hợp phải là ratio-of-sums, không phải trung bình row speed:

```sql
sum(trip_distance_miles) * 3600.0
/ nullif(sum(duration_seconds), 0)
-- filter is_speed_kpi_eligible = true
```

dbt tính độc lập năm cờ: `is_revenue_kpi_eligible`, `is_tip_kpi_eligible`, `is_speed_kpi_eligible`, `is_duration_kpi_eligible`, `is_passenger_kpi_eligible`. `WARNING` không tự động làm mọi KPI mất eligibility.

## 4. Duplicate fingerprint và survivorship

Ba khóa/hash có mục đích khác nhau:

1. `source_record_key = SHA256(source_file_sha256 || ':' || source_row_number)` — idempotency, `UNIQUE NOT NULL`.
2. `raw_exact_fingerprint` — toàn bộ giá trị nguồn ở độ chính xác gốc, `NOT NULL` nhưng không unique.
3. `candidate_trip_fingerprint` — vendor, pickup/dropoff, PU/DO, passenger, distance và total canonical, không unique.

Serialization cho `raw_exact_fingerprint` phải:

- dùng thứ tự cột cố định theo schema version;
- phân biệt null với chuỗi rỗng bằng type/length/null markers;
- không phụ thuộc thứ tự key JSON;
- không chứa batch, ingestion time hay lineage;
- đưa `schema_fingerprint` vào preimage để không so sánh nhầm hai cấu trúc nguồn.

Chỉ auto-quarantine raw-exact duplicate trong cùng file:

```sql
row_number() over (
  partition by source_file_sha256, raw_exact_fingerprint
  order by source_row_number
)
```

Dòng `row_number = 1` tiếp tục qua business DQ. Những dòng sau vào `dq.duplicate_quarantine`, lưu `source_record_key` của survivor. Raw-exact/candidate fingerprint giống nhau giữa các file chỉ warning và giữ cả hai. Không đặt unique constraint cho fingerprint trên toàn Fact.

## 5. Unknown, Not Applicable và SCD

| Dimension | Key 0 | Key -1 | Business unknown member | SCD |
|---|---|---|---|---|
| Date | Unknown date | Not applicable date | Không | Type 1 |
| Time | Unknown time | Not applicable time | Không | Type 1 |
| Zone | Unknown/unmatched zone | Zone not applicable | LocationID 264; LocationID 265 là Outside NYC | Type 1 |
| Vendor | Unknown/unmapped vendor | Vendor not applicable | Không | Type 1 |
| Payment | Unknown/unmapped payment | Payment not applicable | payment code 5 = Unknown | Type 1 |
| Rate | Unknown/unmapped rate | Rate not applicable | rate code 99 = Null/unknown | Type 1 |

Không dùng null foreign key trong Fact. Payment natural code `0` (Flex Fare) phải map sang một surrogate key dương, không map vào technical Unknown key `0`.

Type 2 chỉ được mở lại bằng decision mới khi có snapshot/dictionary có ngày hiệu lực và bằng chứng cùng natural code đổi nghĩa.

## 6. Money reconciliation

### Row-count reconciliation

```text
raw rows = canonical rows + technical quarantine rows

canonical rows
= accepted/warning Fact rows
+ business quarantine rows
+ same-file raw-exact duplicate loser rows
```

Mọi phương trình row count phải bằng tuyệt đối theo file; sai thì fail batch và không commit watermark.

### Amount-eligible reconciliation

- Chỉ gồm dòng cast được `total_amount` sang `numeric(12,2)`.
- Source→canonical và canonical→các nhánh dùng cùng phép cast/round; lệch tối đa `$0.01/file`.
- Technical reject không cast được amount vẫn tham gia row-count reconciliation nhưng không tham gia amount reconciliation.

### Reported total → component total

- Chỉ tính khi các component có mặt theo schema version và không null.
- `abs(diff) <= 0.01`: pass.
- `0.01 < abs(diff) <= 0.05`: warning rounding.
- `abs(diff) > 0.05`: warning review, không tự quarantine.
- Component structurally absent ở 2024 (ví dụ CBD fee) dùng 0; component có cột nhưng null thì đánh `not_reconcilable`, không tự `coalesce` để kết luận cân.

## 7. Power BI role-playing

DDS giữ đúng một `dim_date`, `dim_time`, `dim_zone`. Semantic layer tạo sáu reference/alias:

- `DimPickupDate` → `FactTaxiTrip[pickup_date_key]` (active 1:*);
- `DimDropoffDate` → `FactTaxiTrip[dropoff_date_key]` (active 1:*);
- `DimPickupTime` → `FactTaxiTrip[pickup_time_key]` (active 1:*);
- `DimDropoffTime` → `FactTaxiTrip[dropoff_time_key]` (active 1:*);
- `DimPickupZone` → `FactTaxiTrip[pickup_zone_key]` (active 1:*);
- `DimDropoffZone` → `FactTaxiTrip[dropoff_zone_key]` (active 1:*).

Cross-filter direction: dimension → fact, single direction. Ẩn technical keys khỏi report view. Không dùng một bảng với năm inactive relationships làm thiết kế mặc định.

## 8. Điều kiện ký duyệt

Project Lead và dbt owner kiểm tra các file sau:

- `docs/decisions/PHASE2_DESIGN_FREEZE_PROPOSAL.md`;
- `docs/mapping/INPUT_FILES_2024_2025.md`;
- `docs/mapping/DBT_HANDOVER_CONTRACT.md`.

## 9. Sign-off

| Vai trò | Trạng thái | Ngày | Ghi chú |
|---|---|---|---|
| Project Lead / Integration | **APPROVED** | 2026-11-16 | Người dùng xác nhận “tôi đồng ý kí duyệt” trong phiên làm việc |
| dbt Owner | **APPROVED** | 2026-11-16 | Người dùng xác nhận dbt Owner cũng đã ký duyệt trong phiên làm việc |

Design Freeze đã được khóa. Từ thời điểm này, mọi thay đổi contract phải có Decision Log mới và được hai owner duyệt.

## 10. Làm rõ phạm vi năm sau Design Freeze — 2026-11-17

Người dùng xác nhận hai năm 2024–2025 là quyết định chọn **file đầu vào** của
team, không phải ràng buộc dữ liệu theo `pickup_datetime`. Theo yêu cầu này,
dbt ngừng áp dụng `TIME-006` và không loại chuyến chỉ vì pickup nằm ngoài hai
năm trên. `TIME-005` tiếp tục cảnh báo nếu tháng pickup khác tháng ghi trên
file; các quy tắc về thứ tự/thời lượng chuyến và ngày hiệu lực CBD vẫn áp dụng.
Đây là thay đổi phạm vi rule được người dùng chỉ đạo sau bản ký duyệt ban đầu.
