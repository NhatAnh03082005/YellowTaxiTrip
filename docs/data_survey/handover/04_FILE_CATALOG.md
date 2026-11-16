# 04. Danh mục file và thư mục

## 1. Cây thư mục theo vai trò

| Đường dẫn | Loại | Được tạo bởi | Input | Chức năng | Nên nộp | Tạo lại được |
|---|---|---|---|---|---|---|
| `data/raw/*.parquet` | Raw binary | `download_representative.py` | Manifest + TLC URL | 7 file trip đại diện | Tùy giới hạn dung lượng | Có, cần mạng |
| `data/reference/taxi+_zone_lookup.csv` | Reference CSV | Cung cấp sẵn | TLC lookup | Thuộc tính zone | Có | Tải lại thủ công |
| `data/reference/taxi_zones.zip` | Reference ZIP | Cung cấp sẵn | TLC shape | Gói geometry gốc | Có | Tải lại thủ công |
| `data/reference/taxi_zones_extracted/` | Shapefile | `profile_shapefile.py` nếu chưa có `.shp` | ZIP | Geometry/attributes đã giải nén | Có nếu cần bản đồ | Có |
| `data/reference/cq_BI_PROJECT_LAB_2627.pdf` | PDF | Cung cấp sẵn | Đề bài | Yêu cầu phân tích | Có | Không từ code |
| `data/reference/project_brief_extracted.txt` | Text phụ trợ | Ngoài 11 script | PDF | Text hỗ trợ đọc | Không bắt buộc | Không có script project |
| `data/sample/` | Thư mục trống | `common.init_dirs()` | — | Được dự kiến cho sample data nhưng hiện chưa dùng | Không | Có |
| `scripts/*.py` | Python | Project | — | Pipeline và helper | Có | Source chính |
| `scripts/__pycache__/` | Bytecode | Python tự tạo | Script | Cache import | Không | Có, tự sinh |
| `outputs/profiles/` | CSV evidence | Các profiler/generator | Raw/reference | Manifest, schema, spatial profile | Có | Có |
| `outputs/anomalies/` | CSV/Parquet | DQ/spatial/enrichment/generator | Raw + lookup + shape | Rule, summary, samples | Có | Có |
| `outputs/mappings/` | CSV | `generate_deliverables.py` | Schema + canonical map | STM sơ bộ | Có | Có |
| `outputs/reports/` | Markdown | `generate_deliverables.py` | Tất cả output | Báo cáo cuối | Có | Có |
| `logs/` | Text log | `common.logger()` | Mỗi lần chạy | Audit thao tác thành công | Nên kèm | Có, nhưng nội dung thời điểm khác |
| `README.md` | Markdown | Project | — | Cài đặt và run order | Có | Source |
| `requirements.txt` | Text | Project | — | Dependency tối thiểu | Có | Source |
| `docs/handover/` | Markdown | Tài liệu handover | Code/output thực tế | Giải thích cho người mới | Có | Có thể cập nhật |

## 2. Bộ output 01–12

### 01 — `01_yellow_taxi_file_manifest.csv`

- **Trả lời:** TLC page có link Yellow Taxi nào cho 2009–2026? Link có truy cập được, trùng, 0 byte hay sai tên không?
- **Tạo bởi:** `crawl_tlc_manifest.py:28–60`.
- **Cột quan trọng:** `year`, `month`, `file_name`, `url`, `size_bytes`, `http_status`, `access_status`, `duplicate_url`, `zero_size`, `name_valid`.
- **Người dùng:** Data Engineer chọn source; tester kiểm tra file coverage.
- **Phục vụ:** Source inventory, ETL ingestion control, file-level tests.
- **Ví dụ:** dòng 2009-01 có URL CloudFront, status 200, size 469.759.995, không trùng, không zero, tên hợp lệ. Nghĩa là link xuất hiện trên page và HEAD request thành công.

### 02 — `02_parquet_file_profile.csv`

- **Trả lời:** Mỗi file đại diện có bao nhiêu dòng/cột/row group, dung lượng và checksum nào?
- **Tạo bởi:** `profile_parquet.py:11–33`.
- **Cột:** `file`, `size_bytes`, `row_groups`, `total_rows`, `num_columns`, `columns`, `status`, `selection_reason`, `sha256`.
- **Người dùng:** Data Modeler, ETL developer, reviewer.
- **Phục vụ:** Ước lượng tải, kiểm soát raw, kiểm chứng file đại diện.
- **Ví dụ:** 2009-01 có 14.092.413 dòng, 18 cột, 1 row group và SHA-256 bắt đầu `cb5330...`.

### 03 — `03_schema_by_file.csv`

- **Trả lời:** Từng cột thực tế trong từng file có tên, thứ tự, physical/logical type, null và min/max gì?
- **Tạo bởi:** `profile_parquet.py:18–30`.
- **Cột:** `file`, `column_order`, `column_name`, `physical_type`, `logical_type`, `nullable`, `metadata_null_count`, `metadata_min`, `metadata_max`.
- **Người dùng:** ETL developer và data modeler.
- **Phục vụ:** Version-aware parser, cast rules, schema tests.
- **Ví dụ:** `vendor_name` của 2009 là `BYTE_ARRAY/string`, metadata min CMT và max VTS.

### 04 — `04_schema_evolution.csv`

- **Trả lời:** Một canonical field tương ứng với trường nào ở 2009/2010/…/2026 và cần chuẩn hóa thế nào?
- **Tạo bởi:** `generate_deliverables.schema_evolution()` dòng 16–33.
- **Cột:** `Canonical field`, bảy cột năm, `Thay đổi`, `Quy tắc chuẩn hóa`.
- **Người dùng:** Data architect, ETL developer.
- **Phục vụ:** Staging canonical schema, mapping theo version.
- **Ví dụ:** `vendor_id` map từ `vendor_name[string]` năm 2009, `vendor_id[string]` năm 2010, `VendorID[int]` các năm mới; được đánh dấu rename/case/type changed.

### 05 — `05_taxi_zone_lookup_profile.csv`

- **Trả lời:** Lookup có bao nhiêu dòng, null, distinct; borough/service zone phân bố thế nào?
- **Tạo bởi:** `profile_taxi_zone_csv.py:8–33`.
- **Cột:** `metric_type`, `column`, `row_count`, `dtype`, `null_count`, `distinct_count`, `value`, `count`.
- **Người dùng:** Dimensional modeler, DQ tester.
- **Phục vụ:** Thiết kế và test `dim_zone`.
- **Ví dụ:** `LocationID` có 265 dòng, 265 distinct, min 1, max 265 và unique.

### 06 — `06_taxi_zone_lookup_issues.csv`

- **Trả lời:** Dòng lookup nào có duplicate name, khoảng trắng, special value hoặc vi phạm rule borough/service zone?
- **Tạo bởi:** `profile_taxi_zone_csv.py:17–33`.
- **Cột:** `rule_id`, `issue`, `LocationID`, `Borough`, `Zone`, `service_zone`.
- **Người dùng:** Data steward và người xây dim.
- **Phục vụ:** Chuẩn hóa reference và Unknown member.
- **Ví dụ:** LocationID 56, Zone `Corona` được gắn `ZONE-DUP-NAME`, nghĩa là cùng tên Zone xuất hiện ở nhiều ID; không phải duplicate ID.

### 07 — `07_shapefile_profile.csv`

- **Trả lời:** Shapefile có đủ component, CRS, geometry type, feature, invalid/null/duplicate không?
- **Tạo bởi:** `profile_shapefile.py:19–27`.
- **Cột:** `metric`, `value`, `detail`, `count`.
- **Người dùng:** GIS/BI developer, tester.
- **Phục vụ:** Spatial ETL và web map.
- **Ví dụ:** metric `crs=EPSG:2263`; `feature_count=263`; `geometry_invalid=0`.

### 08 — `08_csv_shapefile_comparison.csv`

- **Trả lời:** LocationID/borough/zone có khớp giữa CSV và shape không?
- **Tạo bởi:** `profile_shapefile.py:28–39`.
- **Cột:** IDs hai phía, `match_status`, borough/zone hai phía, `issues`.
- **Người dùng:** Dimensional modeler, GIS tester.
- **Phục vụ:** Conformed `dim_zone` và geometry bridge.
- **Ví dụ:** LocationID 1 xuất hiện cả hai phía và EWR/Newark Airport khớp. ID 264/265 có `CSV_ONLY`.

### 09 — `09_data_quality_rules.csv`

- **Trả lời:** Có những rule nào, điều kiện gì, severity, tổng số lỗi và recommendation nào?
- **Tạo bởi:** `generate_deliverables.dq_rules()` dòng 51–61, từ `10` cộng hai rule FILE/SCHEMA.
- **Cột:** `Mã rule`, `Nhóm`, `Mô tả`, `Điều kiện kiểm tra`, `Mức độ`, `Số dòng kiểm tra`, `Số dòng lỗi`, `Tỷ lệ`, `Cách xử lý`.
- **Người dùng:** QA, ETL developer, data steward.
- **Phục vụ:** Test catalog và quarantine policy.
- **Ví dụ:** `DQ-AMOUNT-001` kiểm tra `fare IS NULL OR fare < 0`, severity High, tổng 252.336 dòng trên 60.108.247 lượt kiểm tra.

### 10 — `10_anomaly_summary.csv`

- **Trả lời:** Mỗi rule thất bại bao nhiêu dòng trong từng file?
- **Tạo/cập nhật bởi:** `run_data_quality_checks.py`, `profile_legacy_spatial.py`, `recompute_duplicate_counts.py`, `enrich_temporal_fee_checks.py`.
- **Cột:** `file`, `year`, `month`, `rule_id`, `checked_rows`, `failed_rows`, `failed_rate`, `severity`, `recommendation`, `condition`, `description`.
- **Người dùng:** Analyst, QA, nhóm thuyết trình.
- **Phục vụ:** Đánh giá chất lượng theo version và ưu tiên xử lý.
- **Ví dụ:** `DQ-TIME-001` của 2009-01 kiểm tra 14.092.413 dòng, lỗi 0; condition là pickup hoặc dropoff null/unparseable.

### 11 — `11_anomaly_samples.parquet`

- **Trả lời:** Một vài bản ghi thực tế vi phạm rule trông như thế nào?
- **Tạo/cập nhật bởi:** core DQ, legacy spatial và fee enrichment.
- **Cột:** `file`, `year`, `month`, `rule_id`, `sample_json`.
- **Người dùng:** Analyst và người xác minh rule.
- **Phục vụ:** Debug/test mà không xuất hàng triệu dòng lỗi.
- **Ví dụ:** mẫu `DQ-TIME-002` 2009 có pickup 16:08 nhưng dropoff 16:07, duration -1 phút.

### 12 — `12_source_to_target_mapping.csv`

- **Trả lời:** Một source field/version map vào canonical field/type/target và DQ rule nào?
- **Tạo bởi:** `generate_deliverables.mapping()` dòng 36–48.
- **Cột:** source file/field/type, canonical field, staging type, target table/field, transformation, DQ rule.
- **Người dùng:** ETL developer, data architect.
- **Phục vụ:** Source-to-Target Mapping sơ bộ.
- **Ví dụ:** `vendor_name` 2009 → `vendor_id VARCHAR` bằng TRY_CAST, target hiện ghi `fact_taxi_trip`, liên kết `DQ-SCHEMA-001`.

## 3. Các output phụ quan trọng

| File | Ý nghĩa |
|---|---|
| `representative_files.csv` | Inventory 7 file đã chọn, URL, reason, local path, size, SHA-256 |
| `distribution_percentiles.csv` | P50/P90/P95/P99/max duration, distance, speed cho 7 file |
| `legacy_spatial_summary.csv` | 4 dòng: pickup/dropoff cho 2009/2010, số zero/out-of-range/unmatched/matched |
| Báo cáo cuối | Tổng hợp executive, schema, DQ, lineage và đề xuất DDS |

## 4. Log và checksum

Mỗi executable có log cùng tên. `common.py` không có log riêng vì chỉ cung cấp helper. Checksum không nằm ở file `.sha256` riêng; nó là cột trong `representative_files.csv` và được copy sang `02_parquet_file_profile.csv`.
