# KẾ HOẠCH THỰC HIỆN ĐỒ ÁN NYC YELLOW TAXI BI

## 1. Phân công chính

| Người phụ trách | Phạm vi |
|---|---|
| **Bạn - Project Lead/Integration** | Toàn bộ dự án ngoài dbt: quản lý yêu cầu, Docker, PostgreSQL, dữ liệu nguồn, SQL schemas, control/watermark, Mage, Staging, tích hợp, kiểm thử end-to-end, Power BI, báo cáo, bằng chứng và đóng gói |
| **Bạn phụ trách dbt** | Toàn bộ transformation bằng dbt: sources, staging models, intermediate models, Dimensions, FactTaxiTrip, business DQ, dbt tests, reconciliation models, OLAP models và dbt documentation |

Nguyên tắc: hai người dùng chung một repository và một PostgreSQL database. Không tạo project dbt hoặc ETL tách rời khỏi project chính.

---

## 2. Trạng thái giai đoạn 1 - Data Modeling

Các nội dung sau **ĐÃ HOÀN THÀNH**, không thiết kế lại trong giai đoạn 2:

- [x] Khảo sát ba nguồn: Yellow Taxi Trip, Taxi Zone Lookup và Shapefile.
- [x] Lập manifest dữ liệu 2009-2026.
- [x] Profile các file Parquet đại diện.
- [x] Phân tích schema evolution.
- [x] Phân tích null, invalid, outlier, duplicate và spatial issues.
- [x] Xác định business process.
- [x] Xác định grain: một dòng FactTaxiTrip là một chuyến taxi được chấp nhận.
- [x] Thiết kế `DimDate`.
- [x] Thiết kế `DimTime`.
- [x] Thiết kế `DimZone`.
- [x] Thiết kế `DimVendor`.
- [x] Thiết kế `DimPaymentType`.
- [x] Thiết kế `DimRateCode`.
- [x] Xác định role-playing Date, Time và Zone.
- [x] Thiết kế `FactTaxiTrip` và các measures nguyên tử.
- [x] Xác định các measures BI: revenue/trip, speed, tip rate, MoM, YoY và route rank.
- [x] Thiết kế Star Schema và cardinality.
- [x] Thiết kế kiến trúc `Source -> Staging -> DDS`.
- [x] Xây canonical schema sơ bộ.
- [x] Xây DQ rule catalog và nguyên tắc quarantine.
- [x] Ánh xạ mô hình với yêu cầu phân tích cơ bản và nâng cao.
- [x] Hoàn thành báo cáo và source khảo sát giai đoạn 1.

### Không phải làm lại

- Không đổi grain nếu không phát hiện lỗi nghiêm trọng trong dữ liệu thực tế.
- Không tạo thêm Fact khi các câu hỏi vẫn ở grain chuyến taxi.
- Không tách `DimPickupZone` và `DimDropoffZone` thành hai bảng vật lý.
- Không thiết kế lại Star Schema chỉ để bắt đầu ETL.

---

## 3. Design Freeze đầu giai đoạn 2

Các mục sau không phải phần giai đoạn 1 bị thiếu. Đây là quyết định vật lý cần chốt trước khi code ETL.

**Trạng thái cập nhật ngày 2026-11-16:** `APPROVED`. Project Lead và dbt Owner đã ký duyệt; Design Freeze đã được khóa chính thức. Mọi thay đổi contract sau thời điểm này phải có Decision Log mới và được hai owner duyệt.

### Bạn chủ trì, hai người cùng duyệt

- [x] Chọn chính thức hai năm ETL: 2024-2025.
- [x] Kiểm tra đủ file tháng 5, tháng 6 và toàn bộ tháng cần phân tích (24 URL của 2024-2025 trả HTTP 200 khi kiểm tra ngày 2026-11-15; checksum được thực hiện ở 4.1 sau khi tải).
- [x] Chốt revenue mặc định theo Pickup Zone.
- [x] Chốt công thức `revenue_amount`.
- [x] Chốt hard reject, warning và quarantine.
- [x] Chốt duplicate fingerprint và survivorship.
- [x] Chốt Unknown key và Not Applicable key.
- [x] Chốt SCD Type 1 cho Zone, Vendor, Payment và Rate.
- [x] Chốt tip-rate formula và tập chuyến hợp lệ.
- [x] Chốt speed formula và threshold.
- [x] Chốt money reconciliation tolerance.
- [x] Chốt cách role-playing Zone/Date/Time trong Power BI.

### Kết quả cần bàn giao cho người làm dbt

- [x] `Decision Log` đã được cả Project Lead và dbt Owner duyệt.
- [x] Danh sách hai năm và file đầu vào: `docs/mapping/INPUT_FILES_2024_2025.md`.
- [x] Canonical column list và physical types: `docs/mapping/DBT_HANDOVER_CONTRACT.md`.
- [x] DQ rule matrix: rule, severity, action: `docs/mapping/DBT_HANDOVER_CONTRACT.md`.
- [x] Mapping code Vendor/Payment/Rate: `docs/mapping/DBT_HANDOVER_CONTRACT.md`.
- [x] Source-to-Staging mapping: `docs/mapping/DBT_HANDOVER_CONTRACT.md`.
- [x] Staging-to-DDS mapping: `docs/mapping/DBT_HANDOVER_CONTRACT.md`.

### Kết luận mục 3

Mục 3 **ĐÃ HOÀN THÀNH** ngày 2026-11-16. Ba tài liệu Design Freeze, danh sách file đầu vào và dbt handover contract là baseline chính thức để triển khai ETL.

---

## 4. Công việc của bạn - Project Lead/Integration

## 4.1. Quản lý project và dữ liệu

Hướng dẫn thao tác chi tiết: `deployment/runbooks/PHASE2_4_1_4_2_STEP_BY_STEP.md`.

- [x] Duy trì cấu trúc repository duy nhất; local `main` theo dõi `origin/main` tại GitHub.
- [x] Chuyển tài liệu giai đoạn 1 vào đúng `docs/`, `profiling/` và `evidence/`.
- [x] Tạo `.env.example` và `.gitignore` phù hợp.
- [x] Không commit password, volume database hoặc Parquet dung lượng lớn; đã kiểm tra staged files trước khi push.
- [x] Tạo file catalog cho hai năm ETL: `data/catalog/yellow_taxi_2024_2025.csv`.
- [x] Tải và kiểm tra SHA-256, Parquet footer, schema của đủ 24 file Yellow Taxi 2024-2025.
- [ ] Lưu Taxi Zone CSV và Shapefile trong `data/reference/`.
- [ ] Chuẩn bị sample files nhỏ cho unit/integration test.

### Hoàn thành khi

- Thành viên dbt nhận được file/sample/schema mà không phụ thuộc đường dẫn máy của bạn.

## 4.2. Docker và PostgreSQL/PostGIS

Hướng dẫn thao tác chi tiết: `deployment/runbooks/PHASE2_4_1_4_2_STEP_BY_STEP.md`.

- [ ] Tạo `docker-compose.yml`.
- [ ] Tạo Mage service.
- [ ] Tạo PostgreSQL/PostGIS service.
- [ ] Cấu hình port, network và persistent volume.
- [ ] Tạo init scripts.
- [ ] Cấu hình biến môi trường.
- [ ] Kiểm tra `docker compose up` trên môi trường sạch.
- [ ] Viết hướng dẫn `up`, `down`, reset volume và xem log.

### Database schemas cần tạo

- [ ] `stage`.
- [ ] `control`.
- [ ] `dq`.
- [ ] `dds`.
- [ ] `olap`.

### Hoàn thành khi

- Cả bạn và người làm dbt kết nối được cùng PostgreSQL.
- Restart container không làm mất dữ liệu ngoài ý muốn.

## 4.3. Control, metadata và watermark

Bạn chịu trách nhiệm thiết kế và tạo:

- [ ] `control.etl_watermark`.
- [ ] `control.etl_batch`.
- [ ] `control.etl_file`.
- [ ] `control.etl_log`.

Metadata cần lưu:

- [ ] Batch ID và pipeline name.
- [ ] Source file, checksum, year-month và schema version.
- [ ] LSET và CET.
- [ ] Rows read, staged, rejected và loaded.
- [ ] Start/end time.
- [ ] Status và error message.

Watermark flow:

```text
read LSET
-> capture CET once
-> process LSET < source_updated_at <= CET
-> Mage load Staging
-> dbt build
-> dbt tests
-> reconciliation
-> commit LSET = CET
```

- [ ] Không commit watermark nếu dbt hoặc reconciliation thất bại.
- [ ] Chạy lại batch lỗi không tạo duplicate.

## 4.4. Mage pipelines

### Pipeline nguồn tham chiếu

- [ ] Load Taxi Zone Lookup CSV.
- [ ] Load Shapefile/geometry vào PostGIS.
- [ ] Kiểm tra LocationID, CRS và geometry.

### Pipeline Yellow Taxi

- [ ] `get_extraction_window`.
- [ ] `discover_source_files`.
- [ ] `verify_checksum`.
- [ ] `load_source_files`.
- [ ] `canonicalize_schema` ở mức kỹ thuật.
- [ ] `filter_incremental`.
- [ ] `standardize_and_validate` kỹ thuật.
- [ ] `write_technical_quarantine`.
- [ ] `load_stage`.
- [ ] `verify_stage`.

### Quy tắc triển khai Mage

- [ ] Dùng runtime variables, không hard-code path.
- [ ] Đọc theo file/batch/row group, không load tất cả vào RAM.
- [ ] Thêm `source_file`, `source_year_month`, `schema_version`, `batch_id`.
- [ ] Giữ raw values cần đối chiếu.
- [ ] Skip downstream an toàn khi batch rỗng.
- [ ] Ghi audit và counters.
- [ ] Có retry và idempotency.

## 4.5. Physical Staging và technical DQ

Bạn tạo và quản lý:

- [ ] `stage.yellow_taxi_raw`.
- [ ] `stage.yellow_taxi_canonical` hoặc staging tables theo version đã chốt.
- [ ] `stage.taxi_zone_raw`.
- [ ] `stage.taxi_zone_geometry`.
- [ ] Technical quarantine table.

Bạn xử lý:

- File hỏng hoặc không đọc được.
- Schema không nhận diện được.
- Cột bắt buộc kỹ thuật bị thiếu.
- Cast lỗi cần giữ raw value.
- File/checksum đã xử lý.

Business rules như revenue, passenger group, speed, tip rate và accepted/rejected Fact do người làm dbt thực hiện.

## 4.6. Tích hợp Mage với dbt

- [ ] Nhận lệnh chạy dbt đã thống nhất từ người làm dbt.
- [ ] Thêm bước `dbt build` vào master pipeline.
- [ ] Lưu stdout/stderr và exit code.
- [ ] Dừng pipeline khi dbt lỗi.
- [ ] Chạy reconciliation sau dbt.
- [ ] Chỉ commit watermark khi tất cả thành công.
- [ ] Mark batch `SUCCESS` cuối pipeline.

Master pipeline:

```text
get_extraction_window
-> Mage ingestion
-> verify Staging
-> dbt build DDS
-> dbt tests
-> reconciliation
-> dbt build OLAP
-> commit watermark
-> SUCCESS
```

## 4.7. Kiểm thử do bạn phụ trách

- [ ] File discovery test.
- [ ] Checksum test.
- [ ] Schema-version detection test.
- [ ] Extraction-window test.
- [ ] Empty-batch test.
- [ ] Technical rejection test.
- [ ] PostgreSQL connection/load test.
- [ ] Rerun/idempotency test.
- [ ] Failure không commit watermark.
- [ ] End-to-end từ database rỗng.
- [ ] End-to-end incremental file mới.
- [ ] File lỗi rồi retry.
- [ ] Đối soát source-to-stage.
- [ ] Đối soát tổng thể sau khi dbt hoàn tất.

## 4.8. Automation và vận hành

- [ ] Tạo Mage schedule/trigger.
- [ ] Viết runbook backfill.
- [ ] Viết runbook incremental.
- [ ] Viết hướng dẫn retry/recovery.
- [ ] Ghi log và lưu evidence cho ít nhất hai run end-to-end.
- [ ] Chứng minh cùng input không tạo duplicate.

## 4.9. Power BI và dashboard

Bạn phụ trách:

- [ ] Kết nối Power BI với PostgreSQL.
- [ ] Chỉ dùng `dds`/`olap`, không đọc raw file.
- [ ] Tạo semantic relationships.
- [ ] Xử lý role-playing Date, Time và Zone.
- [ ] Tạo hierarchy và measures/DAX cần thiết.
- [ ] Trang Executive Overview.
- [ ] Trang Revenue/Vendor/Payment.
- [ ] Trang Pickup/Dropoff/Top Routes.
- [ ] Trang Passenger/Rate/Distance.
- [ ] Dashboard nâng cao: map, May-vs-June, MoM/YoY và revenue drivers.
- [ ] SQL cross-check các KPI chính.

## 4.10. Báo cáo, evidence và nộp bài

- [ ] Cập nhật báo cáo tích lũy từ giai đoạn 1.
- [ ] Mô tả Docker, Mage, PostgreSQL, dbt, OLAP và Power BI.
- [ ] Ghi rõ phân công và đánh giá thành viên.
- [ ] Lưu ảnh pipeline, database, dbt tests và dashboard.
- [ ] Lưu reconciliation results.
- [ ] Lưu negative-test evidence.
- [ ] Viết demo script.
- [ ] Kiểm tra source không có secret hoặc dữ liệu lớn không cần thiết.
- [ ] Đóng gói đúng tên nhóm/MSSV.

---

## 5. Công việc của bạn phụ trách dbt

## 5.1. Khởi tạo và cấu hình dbt

- [ ] Tạo `dbt_project.yml`.
- [ ] Tạo `profiles.yml.example`; không commit password thật.
- [ ] Cấu hình `dbt-postgres`.
- [ ] Chạy `dbt debug` thành công.
- [ ] Khai báo schemas output `dds`, `dq` và `olap`.

## 5.2. Khai báo sources

- [ ] Khai báo `stage.yellow_taxi_raw/canonical`.
- [ ] Khai báo Zone lookup và geometry sources.
- [ ] Thêm source descriptions.
- [ ] Thêm source tests phù hợp.

## 5.3. Staging models

- [ ] Chuẩn hóa tên cột.
- [ ] Chuẩn hóa null-like values.
- [ ] Cast canonical types.
- [ ] Giữ raw values/cast-error flags cần thiết.
- [ ] Hợp nhất schema của hai năm.
- [ ] Dùng `source()` đúng cách.

## 5.4. Intermediate models

- [ ] Mapping Vendor codes theo version.
- [ ] Mapping Payment Type theo version.
- [ ] Mapping Rate Code theo version.
- [ ] Resolve native/derived LocationID.
- [ ] Tính duration.
- [ ] Tính dữ liệu đầu vào speed và tip rate.
- [ ] Tạo passenger groups, distance bands và time bands.
- [ ] Phát hiện duplicate candidates theo policy.
- [ ] Tạo business validation errors.
- [ ] Tạo accepted-trip dataset.
- [ ] Tạo business quarantine/rejections.

## 5.5. Dimensions

- [ ] `dds.dim_date`.
- [ ] `dds.dim_time`.
- [ ] `dds.dim_zone`.
- [ ] `dds.dim_vendor`.
- [ ] `dds.dim_payment_type`.
- [ ] `dds.dim_rate_code`.
- [ ] Unknown/Not Applicable members.
- [ ] SCD strategy đúng Decision Log.
- [ ] Surrogate keys ổn định theo policy.

## 5.6. FactTaxiTrip

- [ ] Fact đúng grain một chuyến được chấp nhận.
- [ ] Resolve toàn bộ surrogate keys.
- [ ] Có pickup/dropoff DateKey, TimeKey và ZoneKey.
- [ ] Có VendorKey, PaymentTypeKey và RateCodeKey.
- [ ] Có measures nguyên tử đã thiết kế.
- [ ] Có lineage và DQ flags cần thiết.
- [ ] Không để rejected rows lọt vào Fact.
- [ ] Incremental strategy/idempotency phù hợp với batch metadata.

## 5.7. dbt tests

- [ ] `not_null`.
- [ ] `unique`.
- [ ] `relationships`.
- [ ] `accepted_values` khi phù hợp.
- [ ] Fact grain test.
- [ ] Orphan-key test.
- [ ] Accepted/rejected separation test.
- [ ] Cast-error coverage test.
- [ ] Source/Fact/quarantine reconciliation.
- [ ] Amount reconciliation.
- [ ] Tests lỗi phải làm `dbt build` thất bại đúng cách.

## 5.8. OLAP models theo Seminar 3

- [ ] `olap.cube_taxi_activity`.
- [ ] `olap.agg_monthly_revenue`.
- [ ] `olap.agg_zone_activity`.
- [ ] Dùng `LEFT JOIN` phù hợp.
- [ ] Phân biệt `UNKNOWN` và `ALL`.
- [ ] Dùng `GROUPING`/`GROUPING_ID`.
- [ ] Dùng `CUBE` và `ROLLUP`.
- [ ] Chuẩn bị query minh chứng Slice, Dice, Roll-up và Drill-down.

## 5.9. Documentation và bàn giao dbt

- [ ] Mô tả model và columns trong YAML.
- [ ] Chạy `dbt docs generate`.
- [ ] Lưu DAG/manifest/catalog evidence.
- [ ] Cung cấp lệnh chạy đầy đủ cho bạn tích hợp Mage.
- [ ] Ghi rõ model order, selectors và variables.
- [ ] Bàn giao test results và reconciliation outputs.

---

## 6. Ranh giới trách nhiệm giữa hai người

| Hạng mục | Bạn | Bạn dbt |
|---|---:|---:|
| Yêu cầu và Decision Log | Chủ trì | Duyệt |
| Docker/PostgreSQL | Chính | Kiểm tra kết nối |
| Control/watermark/audit | Chính | Đọc batch metadata |
| Download/checksum/file manifest | Chính | Không |
| Mage ingestion | Chính | Không |
| Physical Staging DDL | Chính | Góp ý contract |
| Canonical SQL transformation | Hỗ trợ contract | Chính |
| Business DQ | Cung cấp policy | Chính |
| Dimensions/Fact | Kiểm tra kết quả | Chính |
| dbt tests | Nhận kết quả | Chính |
| End-to-end/retry/idempotency | Chính | Hỗ trợ lỗi dbt |
| OLAP dbt models | Tích hợp | Chính |
| Power BI | Chính | Cung cấp DDS/OLAP contract |
| Báo cáo/evidence/submission | Chính | Cung cấp phần dbt |

---

## 7. Hợp đồng bàn giao giữa hai người

### Bạn bàn giao cho người làm dbt

- PostgreSQL connection variables.
- Stage table names và schema.
- Data dictionary vật lý.
- Sample batch và expected row counts.
- Decision Log.
- DQ/action matrix.
- Batch ID, source file và watermark semantics.

### Người làm dbt bàn giao cho bạn

- Lệnh `dbt debug/build/test/docs generate`.
- Model selectors dùng trong Mage.
- DDS/OLAP table contracts.
- Test failure exit behavior.
- Reconciliation outputs.
- `manifest.json`, `catalog.json` và DAG evidence.

### Integration checkpoint

- [ ] Cả hai chạy cùng một batch ID.
- [ ] Mage load stage thành công.
- [ ] dbt build thành công.
- [ ] dbt tests thành công.
- [ ] Reconciliation đạt.
- [ ] Mage mới commit watermark.

---

## 8. Thứ tự thực hiện khuyến nghị

### Bước 1 - Hai người cùng làm

- Design Freeze và mapping vật lý.

### Bước 2 - Bạn

- Docker, PostgreSQL, schemas, control và sample data.

### Bước 3 - Song song

- Bạn: Mage ingestion và Staging.
- Bạn dbt: khởi tạo project, sources và staging models trên sample contract.

### Bước 4 - Bạn dbt

- Intermediate, Dimensions, Fact và tests.

### Bước 5 - Bạn

- Tích hợp dbt vào Mage, watermark, retry và end-to-end tests.

### Bước 6 - Bạn dbt

- OLAP models và dbt docs.

### Bước 7 - Bạn

- Power BI, phân tích cơ bản/nâng cao, báo cáo và evidence.

### Bước 8 - Hai người

- Review chéo, demo và đóng gói final.

---

## 9. Definition of Done giai đoạn 2 - ETL

- [ ] Docker dựng được trên máy khác.
- [ ] PostgreSQL có `stage`, `control`, `dq`, `dds`.
- [ ] Mage nạp được hai năm và ba nguồn.
- [ ] Có Full/Backfill và Incremental.
- [ ] Có LSET/CET, audit, file manifest và checksum.
- [ ] dbt tạo đủ sáu Dimensions và FactTaxiTrip.
- [ ] dbt tests đạt.
- [ ] Reconciliation cân bằng.
- [ ] Rerun không duplicate.
- [ ] Downstream failure không commit watermark.
- [ ] Schedule/trigger hoạt động.
- [ ] Có logs và evidence cho ít nhất hai run.

---

## 10. Trạng thái tổng hợp

| Giai đoạn | Trạng thái |
|---|---|
| Giai đoạn 1 - Survey và Data Modeling | **HOÀN THÀNH** |
| Design Freeze vật lý cho ETL | **HOÀN THÀNH** - Project Lead và dbt Owner đã ký duyệt |
| Docker/PostgreSQL/Control | Chưa thực hiện |
| Mage/Staging | Chưa thực hiện |
| dbt DDS | Chưa thực hiện |
| Tests/Reconciliation/Automation | Chưa thực hiện |
| OLAP/Power BI/Final | Chưa thực hiện |

