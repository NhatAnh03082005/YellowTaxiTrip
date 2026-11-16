# dbt handover contract — Yellow Taxi 2024–2025

**Contract status:** `APPROVED` — Project Lead và dbt Owner ký ngày 2026-11-16; đồng bộ với `PHASE2_DESIGN_FREEZE_PROPOSAL.md`.  
**Target database:** PostgreSQL/PostGIS.  
**Trusted Fact grain:** một dòng cho một source trip record accepted/warning, sau khi quarantine raw-exact duplicate lặp lại trong cùng file.

## 1. Canonical column list và physical types

Canonical table do Mage tạo: `stage.yellow_taxi_canonical`. Bảng này chỉ chứa canonical source columns, technical cast flags và lineage; không chứa business KPI, business DQ hoặc duplicate classification.

| Canonical column | PostgreSQL type | Null? | Quy tắc |
|---|---|---:|---|
| `stage_row_id` | `bigint generated always as identity` | No | Technical PK |
| `vendor_id` | `smallint` | Yes | `try_cast(VendorID)` |
| `pickup_datetime` | `timestamp without time zone` | Yes | Giờ địa phương New York; không tự gắn UTC |
| `dropoff_datetime` | `timestamp without time zone` | Yes | Giờ địa phương New York |
| `passenger_count` | `smallint` | Yes | Không impute null/0 |
| `trip_distance` | `numeric(10,3)` | Yes | Miles |
| `rate_code_id` | `smallint` | Yes | `try_cast(RatecodeID)` |
| `store_and_fwd_flag` | `char(1)` | Yes | Upper/trim Y/N; giá trị khác giữ raw + warning |
| `pu_location_id` | `smallint` | Yes | Natural TLC LocationID |
| `do_location_id` | `smallint` | Yes | Natural TLC LocationID |
| `payment_type_id` | `smallint` | Yes | Code 0 là Flex Fare, không phải technical unknown |
| `fare_amount` | `numeric(12,2)` | Yes | USD |
| `extra` | `numeric(12,2)` | Yes | USD |
| `mta_tax` | `numeric(12,2)` | Yes | USD |
| `tip_amount` | `numeric(12,2)` | Yes | USD; cash tips không đầy đủ |
| `tolls_amount` | `numeric(12,2)` | Yes | USD |
| `improvement_surcharge` | `numeric(12,2)` | Yes | USD |
| `total_amount` | `numeric(12,2)` | Yes | Reported amount; nguồn cho revenue |
| `congestion_surcharge` | `numeric(12,2)` | Yes | USD |
| `airport_fee` | `numeric(12,2)` | Yes | Source có thể viết `Airport_fee`; normalize lowercase |
| `cbd_congestion_fee` | `numeric(12,2)` | Yes | Structurally unavailable ở 2024; có từ 2025 |
| `cast_error_flags` | `jsonb` | No | Default `{}` |
| `raw_payload` | `jsonb` | Yes | Bắt buộc cho cast-error; optional cho accepted row nếu raw table đã immutable |
| `batch_id` | `bigint` | No | FK control batch |
| `source_file` | `varchar(255)` | No | File name |
| `source_year_month` | `char(7)` | No | Từ file name, format YYYY-MM |
| `source_file_sha256` | `char(64)` | No | Idempotency/file identity |
| `source_row_number` | `bigint` | No | Ổn định trong file; có thể derive từ row group + row index |
| `source_record_key` | `char(64)` | No | SHA-256 của `file_sha256:row_number`; unique bắt buộc |
| `schema_fingerprint` | `char(64)` | No | SHA-256 của ordered column names + logical types |
| `schema_version` | `varchar(20)` | No | Version tra từ schema fingerprint, không suy ra chỉ từ năm |
| `source_last_modified` | `timestamptz` | Yes | HTTP metadata |
| `ingested_at` | `timestamptz` | No | Audit time |

Raw code fields bị cast lỗi phải còn trong `raw_payload`; dbt không được suy ngược raw value từ null canonical.

`source_record_key` được serialization ổn định như sau:

```text
SHA256(lower(source_file_sha256) || ':' || decimal(source_row_number))
```

Đặt `UNIQUE NOT NULL` trên `source_record_key`. Không dùng business fingerprint để chống rerun.

### dbt intermediate fields

Các cột dưới đây do dbt tạo trong `int_taxi_trip_enriched` hoặc `int_taxi_trip_classified`, không thuộc physical canonical của Mage:

| dbt field | Type | Model/responsibility |
|---|---|---|
| `duration_seconds` | `integer` | `int_taxi_trip_enriched` |
| `row_speed_mph` | `numeric(10,2)` | `int_taxi_trip_enriched` |
| `reported_total_amount` | `numeric(12,2)` | `int_taxi_trip_enriched` |
| `calculated_total_amount` | `numeric(12,2)` | `int_taxi_trip_enriched` |
| `revenue_amount` | `numeric(12,2)` | `int_taxi_trip_classified` |
| `row_tip_rate` | `numeric(12,6)` | tip/fare diagnostic; KPI dùng ratio-of-sums |
| `raw_exact_fingerprint` | `char(64)` | raw values ở precision gốc; non-unique |
| `candidate_trip_fingerprint` | `char(64)` | business duplicate candidate; non-unique |
| `primary_disposition` | `varchar(24)` | business classification |
| `dq_rule_ids` | `text[]` | business DQ flags |
| `is_revenue_kpi_eligible` | `boolean` | độc lập với disposition tổng quát |
| `is_tip_kpi_eligible` | `boolean` | payment=1, fare>0, tip>=0, không void/reversal |
| `is_speed_kpi_eligible` | `boolean` | duration/distance dương, speed<=45, không rule loại speed |
| `is_duration_kpi_eligible` | `boolean` | duration dương và <=180 phút |
| `is_passenger_kpi_eligible` | `boolean` | passenger_count trong 1..6 |

Luồng ownership bắt buộc:

```text
Mage: stage.yellow_taxi_raw
   -> stage.yellow_taxi_canonical (technical only)
dbt: stg_yellow_taxi
   -> int_taxi_trip_enriched
   -> int_taxi_trip_classified
   -> dds.fact_taxi_trip / dq business tables
```

`raw_exact_fingerprint` được dbt tính từ `stage.yellow_taxi_raw` (join canonical bằng `source_record_key`) để không mất precision hoặc representation nguồn. Không bắt buộc nhân bản toàn bộ raw values vào canonical.

## 2. DQ rule matrix

Thứ tự chọn disposition khi một dòng vi phạm nhiều rule:

```text
TECHNICAL_REJECT > DUPLICATE > BUSINESS_QUARANTINE > WARNING > ACCEPTED
```

| Rule | Điều kiện | Severity | Action |
|---|---|---|---|
| FILE-001 | File HTTP lỗi, rỗng, footer Parquet hỏng | Hard reject | Reject file; fail batch; giữ metadata/error |
| FILE-002 | SHA-256 khác manifest đã khóa cho cùng filename/version | Stop/review | Gắn `SOURCE_VERSION_CHANGED`, dừng batch; nếu TLC revision hợp lệ thì tạo file version mới và backfill có kiểm soát |
| SCHEMA-001 | Thiếu required column của schema version | Hard reject | Reject file; fail batch |
| SCHEMA-002 | Schema fingerprint chưa có trong registry | Stop/review | Không suy version từ năm; review và đăng ký version mới trước khi load |
| CAST-001 | Pickup/dropoff không parse được | Hard reject row | Technical quarantine; không vào canonical trusted set |
| CAST-002 | PU/DO LocationID không cast integer | Hard reject row | Technical quarantine |
| CAST-003 | `total_amount` không cast numeric | Hard reject row | Technical quarantine |
| TIME-001 | Pickup/dropoff null | High | Business quarantine |
| TIME-002 | Dropoff <= pickup | High | Business quarantine |
| TIME-003 | Duration > 24 giờ | High | Business quarantine |
| TIME-004 | 3 giờ < duration <= 24 giờ | Warning | Load Fact; exclude duration/speed KPI mặc định |
| TIME-005 | Pickup month khác file month nhưng vẫn trong 2024–2025 | Warning | Load + flag; month role lấy từ pickup datetime |
| TIME-006 | Pickup nằm ngoài 2024–2025 | High | Business quarantine; không để rò sang kỳ ngoài scope |
| DIST-001 | Distance < 0 | High | Business quarantine |
| DIST-002 | Distance null hoặc = 0 | Warning | Load; speed null; exclude distance/speed KPI mặc định |
| SPEED-001 | 45 < speed <= 80 mph | Warning | Load; exclude speed KPI mặc định |
| SPEED-002 | Speed > 80 mph | High | Business quarantine |
| PASS-001 | Passenger null, 0 hoặc > 6 | Warning | Load; không impute; exclude passenger KPI khi null/0 |
| LOC-001 | PU/DO null hoặc không tồn tại trong lookup | Warning | Load với zone key 0; giữ natural value/raw |
| LOC-002 | LocationID 264 hoặc 265 | Info | Load member nguồn hợp lệ; map surrogate dương |
| CODE-001 | Vendor/payment/rate null hoặc ngoài mapping | Warning | Load với dimension key 0 |
| CODE-002 | Store flag ngoài Y/N | Warning | Load, canonical null, giữ raw |
| CODE-003 | Payment code 6 = Voided trip | High | Business quarantine; không tính trip/revenue |
| AMT-001 | Total null hoặc <= 0 | High | Business quarantine; revenue null |
| AMT-002 | Fare hoặc component tiền âm | High | Business quarantine; `reversal_or_adjustment_candidate` |
| AMT-003 | Tip < 0 hoặc tip > total | High | Business quarantine |
| AMT-004 | Tip > 0 với payment khác credit card | Warning | Load + flag; không vào recorded-card tip KPI |
| AMT-005 | Component-complete và abs(total − component sum) <= $0.01 | Pass | Load |
| AMT-006 | Component-complete và $0.01 < abs(diff) <= $0.05 | Warning | Load; rounding warning |
| AMT-007 | Component-complete và abs(diff) > $0.05 | Warning | Load; review, không quarantine chỉ vì mismatch |
| AMT-008 | Component expected nhưng null | Warning | `not_reconcilable`; không coalesce null thành 0 |
| FEE-001 | Airport fee >0 ngoài PU 132/138 | Warning | Load + monitor; chưa đủ căn cứ quarantine |
| FEE-002 | CBD fee >0 trước 2025-01-05 | High | Business quarantine sau khi xác nhận timestamp/file hợp lệ |
| DUP-001 | `raw_exact_fingerprint` lặp trong cùng `source_file_sha256`, occurrence > 1 theo source row | High | Duplicate quarantine; liên kết source_record_key của winner |
| DUP-002 | Raw-exact/candidate fingerprint giống nhau giữa các file | Warning | Giữ cả hai; không auto-delete |

Không dùng P99 làm action threshold. P99 chỉ ghi vào monitoring table theo file/tháng.

## 3. Mapping code Vendor / Payment / Rate

Tất cả mapping phải là dbt seeds có test unique/not-null.

### Vendor

| Natural code | Name | Action |
|---:|---|---|
| 1 | Creative Mobile Technologies, LLC | Map member dương |
| 2 | Curb Mobility, LLC | Map member dương |
| 6 | Myle Technologies Inc | Map member dương |
| 7 | Helix | Map member dương |
| null/other | Technical Unknown | `vendor_key = 0`, warning |

### Payment type

| Natural code | Name | Action |
|---:|---|---|
| 0 | Flex Fare trip | Map member dương |
| 1 | Credit card | Map member dương; eligible tip-rate |
| 2 | Cash | Map member dương; không tính recorded-card tip-rate |
| 3 | No charge | Map member dương |
| 4 | Dispute | Map member dương |
| 5 | Unknown | Business member dương, không phải technical key 0 |
| 6 | Voided trip | Map member dương trong dimension; row business quarantine, không vào trusted Fact |
| null/other | Technical Unknown | `payment_type_key = 0`, warning |

### Rate code

| Natural code | Name | Action |
|---:|---|---|
| 1 | Standard rate | Map member dương |
| 2 | JFK | Map member dương |
| 3 | Newark | Map member dương |
| 4 | Nassau or Westchester | Map member dương |
| 5 | Negotiated fare | Map member dương |
| 6 | Group ride | Map member dương |
| 99 | Null/unknown | Business member dương, không phải technical key 0 |
| null/other | Technical Unknown | `rate_code_key = 0`, warning |

## 4. Source-to-Staging mapping

| Source Parquet | Canonical | Transform |
|---|---|---|
| `VendorID` | `vendor_id` | try cast smallint; raw retained on error |
| `tpep_pickup_datetime` | `pickup_datetime` | try cast timestamp; New York local wall time |
| `tpep_dropoff_datetime` | `dropoff_datetime` | try cast timestamp; New York local wall time |
| `passenger_count` | `passenger_count` | try cast smallint; do not impute |
| `trip_distance` | `trip_distance` | round/cast numeric(10,3) |
| `RatecodeID` | `rate_code_id` | try cast smallint |
| `store_and_fwd_flag` | `store_and_fwd_flag` | upper(trim()); only Y/N canonical |
| `PULocationID` | `pu_location_id` | try cast smallint |
| `DOLocationID` | `do_location_id` | try cast smallint |
| `payment_type` | `payment_type_id` | try cast smallint |
| `fare_amount` | `fare_amount` | round/cast numeric(12,2) |
| `extra` | `extra` | round/cast numeric(12,2) |
| `mta_tax` | `mta_tax` | round/cast numeric(12,2) |
| `tip_amount` | `tip_amount` | round/cast numeric(12,2) |
| `tolls_amount` | `tolls_amount` | round/cast numeric(12,2) |
| `improvement_surcharge` | `improvement_surcharge` | round/cast numeric(12,2) |
| `total_amount` | `total_amount` | round/cast numeric(12,2) |
| `congestion_surcharge` | `congestion_surcharge` | round/cast numeric(12,2) |
| `Airport_fee`/`airport_fee` | `airport_fee` | case-insensitive name mapping; numeric(12,2) |
| absent in 2024 / `cbd_congestion_fee` in 2025 | `cbd_congestion_fee` | 2024 structural absence recorded in schema version; canonical null |
| file/control metadata | lineage columns | no business transform; tạo `source_record_key` và tra `schema_version` từ schema fingerprint |

Required source columns for file acceptance: pickup, dropoff, PU, DO, total. Các cột khác có thể nullable nhưng phải hiện diện đúng schema version; CBD được phép absent ở 2024.

## 5. Staging-to-DDS mapping

Fact target: `dds.fact_taxi_trip`.

| Canonical staging | DDS target | Transform/join |
|---|---|---|
| dbt load | `trip_key` | `bigint generated always as identity primary key` |
| `source_record_key` | `source_record_key` | Pass-through; `UNIQUE NOT NULL` trong trusted Fact |
| `raw_exact_fingerprint` | `raw_exact_fingerprint` | Pass-through; non-unique |
| `candidate_trip_fingerprint` | `candidate_trip_fingerprint` | Pass-through; non-unique |
| pickup date | `pickup_date_key` | Join `dim_date.full_date`; fallback 0 |
| dropoff date | `dropoff_date_key` | Join `dim_date.full_date`; fallback 0 |
| pickup time | `pickup_time_key` | Join `dim_time` ở grain minute; fallback 0 |
| dropoff time | `dropoff_time_key` | Join `dim_time` ở grain minute; fallback 0 |
| `pu_location_id` | `pickup_zone_key` | Join `dim_zone.location_id`; fallback 0 |
| `do_location_id` | `dropoff_zone_key` | Join `dim_zone.location_id`; fallback 0 |
| `vendor_id` | `vendor_key` | Join Type-1 `dim_vendor`; fallback 0 |
| `payment_type_id` | `payment_type_key` | Join Type-1 `dim_payment_type`; fallback 0 |
| `rate_code_id` | `rate_code_key` | Join Type-1 `dim_rate_code`; fallback 0 |
| constant | `trip_count` | 1 |
| `passenger_count` | `passenger_count` | Pass-through |
| `trip_distance` | `trip_distance_miles` | Pass-through |
| `duration_seconds` | `duration_seconds` | Pass-through |
| `row_speed_mph` | `row_speed_mph` | Pass-through; không dùng `AVERAGE()` làm KPI tổng hợp |
| all amount columns | same-named atomic measures | Pass-through numeric(12,2) |
| `revenue_amount` | `revenue_amount` | Positive reported total for accepted/warning |
| `row_tip_rate` | `row_tip_rate` | Row diagnostic; aggregate KPI = `SUM(tip)/SUM(fare)` |
| five eligibility flags | same-named boolean columns | Pass-through; Power BI filters theo KPI cụ thể |
| `store_and_fwd_flag` | `store_and_fwd_flag` | Degenerate attribute |
| `source_file`, `source_row_number`, `batch_id` | lineage columns | Pass-through |
| `dq_rule_ids` | `dq_rule_ids` | Pass-through for warning rows |

Only `primary_disposition IN ('ACCEPTED','WARNING')` enters Fact. Technical rejects, duplicate losers, and business quarantine remain in `dq.*` tables.

## 6. dbt tests bắt buộc

- Unique/not-null `source_record_key` ở canonical và trusted Fact.
- `raw_exact_fingerprint` và `candidate_trip_fingerprint` not-null nhưng không unique.
- Fact `trip_key` identity unique/not-null; tất cả FK not-null.
- Relationships từ Fact đến sáu dimensions.
- Accepted/warning separation: không có quarantined `source_record_key` trong Fact.
- Raw count = canonical count + technical quarantine count, chính xác theo file.
- Canonical count = Fact + business quarantine + same-file duplicate loser, chính xác theo file.
- Amount reconciliation chỉ chạy trên `total_amount` cast-eligible rows; lệch <= $0.01/file.
- Same-file duplicate winner là source row nhỏ nhất; cross-file fingerprint match không bị loại.
- Eligibility flags đúng predicate và không được suy ra chỉ từ `primary_disposition`.
- Seed mappings unique theo natural code; technical member 0 và N/A member -1 tồn tại đúng một lần.
- Zone natural IDs 264 và 265 map surrogate dương; geometry null được phép.
- 2024 `cbd_congestion_fee` structurally absent/null; 2025 column recognized.

## 7. Các thay đổi bắt buộc so với mapping profiling cũ

- Không map source thẳng vào Fact; luôn qua canonical staging và DQ disposition.
- Đổi tiền từ floating-point sang `numeric(12,2)`.
- Tách natural codes khỏi surrogate keys.
- Tách `total_amount` (reported) khỏi `calculated_total_amount`.
- Không reject theo P99.
- Không coi payment code 0 là invalid; map Flex Fare.
- Không coi Zone 264 là technical Unknown key.

## 8. Fingerprint serialization contract

### Raw-exact fingerprint

Preimage phải được dựng từ `schema_fingerprint` và danh sách cột nguồn theo thứ tự ordinal trong Parquet schema. Mỗi value dùng encoding có type tag, null marker và length prefix; null phải khác empty string. Không hash trực tiếp JSON text vì thứ tự key/format có thể không ổn định.

```text
raw_exact_fingerprint = SHA256(
  schema_fingerprint || encoded_raw_column_1 || ... || encoded_raw_column_n
)
```

Không đưa `batch_id`, `ingested_at`, file metadata hoặc source row position vào raw-exact fingerprint.

### Same-file duplicate ranking

```sql
row_number() over (
  partition by source_file_sha256, raw_exact_fingerprint
  order by source_row_number
) as duplicate_occurrence_number
```

- Occurrence 1 tiếp tục qua business DQ.
- Occurrence >1 vào `dq.duplicate_quarantine`.
- Không dùng `ingested_at` để chọn winner.
- Không đặt unique constraint cho hai fingerprint trên toàn Fact.

### KPI aggregate contracts

```sql
-- Recorded credit-card tip rate
SUM(tip_amount) / NULLIF(SUM(fare_amount), 0)
-- filter is_tip_kpi_eligible = true

-- Fleet/selection speed
SUM(trip_distance_miles) * 3600.0
/ NULLIF(SUM(duration_seconds), 0)
-- filter is_speed_kpi_eligible = true
```

Không dùng `AVERAGE(row_tip_rate)` hoặc `AVERAGE(row_speed_mph)` làm KPI tổng hợp mặc định.
