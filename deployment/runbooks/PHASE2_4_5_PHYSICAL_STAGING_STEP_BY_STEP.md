# Hướng dẫn triển khai mục 4.5 — Physical Staging và technical DQ

**Owner:** Project Lead/Integration  
**Ngày triển khai:** 2026-11-17 (Asia/Bangkok)  
**Trạng thái:** HOÀN TẤT DDL, CONSTRAINTS VÀ INDEXES; chưa chạy data-load integration  
**Schema thống nhất:** `stage` cho staging, `dq` cho quarantine.

## 1. Phạm vi đã triển khai

Năm bảng vật lý:

```text
stage.taxi_zone_raw
stage.taxi_zone_geometry
stage.yellow_taxi_raw
stage.yellow_taxi_canonical
dq.technical_quarantine
```

File triển khai:

```text
sql/stage/
├── 001_stage_tables.sql
├── 002_stage_indexes.sql
└── 003_stage_comments.sql

tests/stage/
├── test_stage_schema.sql
├── test_stage_constraints.sql
└── test_stage_indexes.sql
```

## 2. Quyết định physical design

- Không tạo schema `staging_raw`; tên đầy đủ là `stage.yellow_taxi_raw`.
- Raw table giữ tên và kiểu gần nguồn để không mất representation trước dbt fingerprint.
- Canonical table khớp `docs/mapping/DBT_HANDOVER_CONTRACT.md`.
- Tiền canonical dùng `numeric(12,2)`; distance dùng `numeric(10,3)`.
- Pickup/dropoff là New York local wall time, lưu `timestamp without time zone`.
- `source_record_key` unique/not-null trên raw, canonical và technical quarantine.
- `source_record_key` không chứa `batch_id`, do đó retry idempotent.
- Technical quarantine giữ cả `raw_payload` và `cast_error_flags`.
- Geometry là `MultiPolygon`, SRID 2263; chỉ 263 geometry là hợp lệ.
- Zone 264/265 vẫn tồn tại trong lookup nhưng không có polygon.

## 3. Reference tables

`stage.taxi_zone_raw` có primary key `location_id`, range 1–265 và các thuộc tính borough, zone, service zone.

`stage.taxi_zone_geometry` có:

```text
location_id -> FK stage.taxi_zone_raw
geometry    -> geometry(MultiPolygon, 2263)
```

Constraints chặn geometry null, empty, invalid hoặc sai SRID. GiST index hỗ trợ spatial query.

Exporter `load_reference_stage.py` đã được sửa thành một câu lệnh:

```sql
TRUNCATE TABLE stage.taxi_zone_geometry, stage.taxi_zone_raw;
```

Điều này cho phép reload hai bảng trong cùng transaction khi có foreign key.

## 4. Yellow Taxi raw table

`stage.yellow_taxi_raw` giữ source fields và lineage. Source columns có tên gốc như `"VendorID"`, `"RatecodeID"`, `"PULocationID"`, `"DOLocationID"` và `"Airport_fee"` để tương thích trực tiếp với DataFrame của Mage.

Lineage bắt buộc:

```text
batch_id
source_file
source_year_month
source_file_sha256
source_row_number
source_record_key
schema_fingerprint
schema_version
source_last_modified
ingested_at
```

Checks enforce month format, lowercase SHA-256, positive row number và schema version v001/v002.

## 5. Yellow Taxi canonical table

`stage.yellow_taxi_canonical` chỉ chứa technical canonical fields. Bảng không chứa revenue, duration, speed, business disposition hoặc KPI flags.

Contract chính:

- identity `stage_row_id`;
- canonical code/time/numeric fields;
- JSONB `cast_error_flags` và `raw_payload`;
- đầy đủ source lineage;
- FK `batch_id -> control.etl_batch`;
- unique `source_record_key`.

## 6. Technical quarantine

`dq.technical_quarantine` nhận dòng không cast được required technical fields. `cast_error_flags` phải là JSON object khác `{}` và `raw_payload` phải là JSON object.

Business anomalies như amount âm, duration sai, speed cao hoặc passenger ngoài range không thuộc bảng này.

## 7. Idempotent loader compatibility

`mage_project/utils/stage_loader.py` đã được cập nhật để cast mỗi temporary-table column về đúng physical target type trước khi insert. Việc này xử lý an toàn các trường hợp như text -> timestamptz, bigint -> smallint và float -> numeric.

Insert dùng:

```sql
ON CONFLICT (source_record_key) DO NOTHING
```

Không dùng `if_exists='replace'`; Mage không được phép làm mất constraints/indexes.

## 8. Indexes

Đã tạo 15 indexes:

- 1 GiST spatial index cho zone geometry.
- Raw: batch, file SHA, year-month, `(file SHA, source row)`.
- Canonical: batch, file SHA, year-month, pickup datetime, PU và DO location.
- Technical quarantine: batch, file SHA, year-month và GIN trên error flags.

Các unique indexes do unique constraints tạo tự động không bị nhân bản bằng index thủ công.

## 9. Cách chạy migration

```powershell
Get-Content -Raw .\sql\stage\001_stage_tables.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U nyc_taxi -d nyc_taxi

Get-Content -Raw .\sql\stage\002_stage_indexes.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U nyc_taxi -d nyc_taxi

Get-Content -Raw .\sql\stage\003_stage_comments.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U nyc_taxi -d nyc_taxi
```

DDL đã được chạy hai lần thành công; lần hai chỉ trả `already exists, skipping`.

## 10. Cách chạy tests

```powershell
Get-Content -Raw .\tests\stage\test_stage_schema.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U nyc_taxi -d nyc_taxi

Get-Content -Raw .\tests\stage\test_stage_constraints.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U nyc_taxi -d nyc_taxi

Get-Content -Raw .\tests\stage\test_stage_indexes.sql |
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U nyc_taxi -d nyc_taxi
```

Kết quả hiện tại:

```text
PASS: task 4.5 table and canonical schema contract
PASS: task 4.5 constraints and source-key uniqueness
PASS: task 4.5 required indexes
```

Mage regression tests: 11/11 pass.

Live loader smoke test cũng pass: cùng một DataFrame được gửi hai lần qua
`upsert_dataframe`, target casts gồm text -> timestamptz hoạt động và database
chỉ giữ một `source_record_key`.

## 11. Việc tiếp theo

Phần physical schema/index đến mục 8 đã hoàn tất. Chưa thực hiện trong checkpoint này:

1. Chạy `pl_reference_taxi_zone` và xác nhận 265/263 rows.
2. Chạy Yellow Taxi subset 2024-05.
3. Kiểm raw = canonical + technical quarantine.
4. Test retry/rerun thật qua Mage.
5. Thu full integration evidence rồi đóng 4.4 và 4.5 end-to-end.
