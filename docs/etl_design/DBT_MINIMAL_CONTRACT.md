# Contract dbt tối giản — v1

Ngày: 2026-11-19. Trạng thái: **T03–T05 đã triển khai opt-in và kiểm SQL QA; chưa migrate live, T06–T12 còn lại**.
Thẩm quyền phạm vi: người dùng yêu cầu tối giản và giao thực hiện Cụm 1. Không tự ghi chữ ký/approval của owner khác.

## 1. Nguồn yêu cầu và phạm vi thay thế

Đọc cùng `DBT_MINIMAL_REFACTOR_TASK_PLAN.md` và `reports/dbt_minimal_refactor/T00.md`.
Nguồn yêu cầu là bản tổng hợp `D:/BI/DATH/Yeu_cau_do_an_va_noi_dung_3_buoi_seminar.md`; chưa đọc lại PDF/video gốc. Phạm vi giữ 24 file 2024–2025, ba nguồn và đầy đủ yêu cầu phân tích. Mẫu nhỏ không thay thế dữ liệu hai năm.

Contract này thay thế CHO PIPELINE TỐI GIẢN các mục cũ về registry fingerprint, cross-file warning/history, lifecycle archive, Fact columns dư và full tests mỗi batch trong:

- `docs/mapping/DBT_HANDOVER_CONTRACT.md`: phần fingerprints persistent/cross-file và lifecycle; giữ canonical Mage, mapping business codes, thresholds và KPI.
- `docs/decisions/PHASE2_DESIGN_FREEZE_PROPOSAL.md`: DF-05 đổi phạm vi detection từ lịch sử sang in-file; vẫn giữ idempotency bằng source identity, không xóa theo candidate. DF-01..04,06..12 còn áp dụng, trừ thay đổi được ghi rõ dưới đây.
- `docs/etl_design/STORAGE_OPTIMIZATION_FINAL_24_MONTHS.md`: không triển khai candidate history interface/cutover registry theo proposal cũ; legacy decommission là việc riêng sau validation.
- `dbt_project/README.md` và docs lifecycle: hành vi archive/guard cũ chỉ mô tả phiên bản legacy.

Không sửa canonicalization Mage hay thuật toán source identity ở Cụm 1. Không xóa dữ liệu legacy. T03–T09 phải cập nhật code/docs/DDL đồng bộ; không dùng contract này để tuyên bố runtime hiện tại đã thay đổi.

## 2. Scope, identity, Fact loading

| Quyết định | Contract bắt buộc |
|---|---|
| File scope | Một batch xử lý một immutable file_version_id; runner truyền load_batch_id và version, kiểm raw/canonical/technical scope trước dbt |
| File identity | source_file + checksum đã xác minh → control.source_file_version; không suy version từ tên tháng |
| Source key | Giữ SHA256(lower(file_sha256) + ':' + decimal(source_row_number)); positive 1-based row number, không dùng business hash làm PK |
| Grain | Một source record ACCEPTED/WARNING sau in-file exact dedup; trip_count=1 |
| Fact identity | bigint identity trip_key, physical PK; source_record_key UNIQUE NOT NULL |
| Loading | **Simple upsert theo source key**, chỉ version đang xử lý; so giá trị nghiệp vụ để tránh UPDATE vô ích, không đổi trip_key cho source key vẫn có mặt |
| Stale rows | Anti-join current accepted keys trong đúng processing version để loại stale Fact; đồng bộ DQ theo version, kể cả output rỗng |
| Retry | FAILED/STAGED cùng version: dựng lại workspace, upsert/dọn stale theo scope; không dựa bảng tạm cũ. Fact key của nguồn đang accepted phải ổn định |
| Identity giới hạn | Không còn lịch sử để phục hồi trip_key cho nguồn đã bị loại khỏi Fact rồi được chấp nhận lại. Trong trường hợp đó có thể cấp trip_key mới; không hứa stable resurrection identity |
| SUCCESS unchanged | Skip trước dbt; pending cleanup chỉ retry cleanup và chặn batch mới nếu chưa xong |
| Revised file | Chỉ replacement request tường minh gắn old/new version mới được retire old. Checksum đổi đơn thuần phải dừng, không tự xóa |
| Concurrency | Một writer cho staging/workspace; khóa bảo vệ scope và Fact mutations. Không bỏ writer lock cùng archive |

Không chọn delete/insert cả Fact version cho rerun thông thường: upsert đơn giản bảo toàn key và tránh ghi lại các dòng không đổi, trong khi lịch sử đã được bỏ. Không cần giữ key ổn định qua hai checksum khác nhau vì đó là source identity mới.

T05 transaction phải giữ Fact mutations của một version/replacement nhất quán. Fact/DQ/OLAP models không tự động chung một transaction; lỗi giữa models được runner roll-forward retry, consumer chỉ refresh sau SUCCESS. Không tuyên bố reader trực tiếp luôn thấy atomic snapshot.

## 3. Duplicate và fingerprint

1. Giữ thuật toán raw-exact fingerprint 19/20 field của legacy, ordered fields/types, null markers, byte length UTF-8 và schema_fingerprint prefix. Golden v001/v002 cũ tiếp tục là contract. Không hash canonical rounded values thay cho raw.
2. Chỉ xét canonical records đủ điều kiện technical; trong một file checksum, cùng raw-exact hash thì source_row_number nhỏ nhất thắng. Dòng còn lại vào duplicate quarantine với survivor key/occurrence. DQ precedence: TECHNICAL_REJECT > DUPLICATE > BUSINESS_QUARANTINE > WARNING > ACCEPTED.
3. Giữ candidate fingerprint 8 field theo legacy nhưng chỉ trong query/workspace. Ghi `has_in_file_candidate_match` khi có ít nhất hai canonical records cùng candidate trong file; summary ghi số dòng có flag và số nhóm. Đây là **diagnostic**, không tự thêm WARNING hay loại dòng, không đổi eligible KPI.
4. Candidate giống nhưng raw-exact khác: giữ cả hai nếu business rules cho phép. Exact duplicate vẫn có thể có candidate flag; không cộng candidate counts vào excluded rows.
5. Không phát hiện cross-file trong runtime mới; cùng hash ở file khác giữ cả hai, không tự thêm DUP-002. Không hứa phát hiện mọi duplicate giữa hai năm.
6. Không lưu raw/candidate hash trên Fact hoặc business quarantine. Duplicate quarantine giữ loser source key, survivor key và occurrence; có thể truy lại raw từ immutable Parquet. Workspace hash có thể giữ hex hiện tại để giảm rủi ro; không đổi kiểu/serialization trong T03.
7. Nếu raw values cần hash bị thiếu: fail contract cho file; không gom các null hash thành duplicate. Không suy raw từ canonical đã mất precision.

## 4. Quy tắc nghiệp vụ và KPI giữ nguyên

Nguồn chuẩn: `dbt_project/dbt_project.yml`, `seeds/dq_rule_policy.csv`, `int_taxi_trip_enriched.sql` và `int_taxi_trip_classified.sql` tại c679360. T02 fixture oracle đóng băng các ví dụ; không phải bản thay thế toàn rule engine.

- Timestamp là local New York không gắn timezone; schema v001/v002 tra fingerprint thực tế. CBD structurally absent 2024, expected từ schema 2025; ngày hiệu lực 2025-01-05.
- Duration >1440 phút quarantine; >180..1440 warning. Row speed round(distance*3600/duration,2), >80 quarantine, >45..80 warning. Eligible speed cần trusted, distance/duration dương, duration<=180 phút, rounded speed<=45.
- Passenger 1..6 eligible; null/0/>6 warning, không impute; negative passenger cũng warning theo rule code hiện tại. Nhóm 1 SOLO, 2..4 SMALL_GROUP, >=5 LARGE_GROUP; invalid có nhóm riêng.
- Trip distance âm quarantine, null/0 warning. Void payment=6 quarantine. Code/zone không map → key 0 + warning; 264/265 là business zones, key dương; key -1 chỉ Not Applicable.
- Total null/<=0, component âm, tip âm/>total quarantine theo rules hiện tại. Component expected null không tự coalesce 0; CBD structurally absent được tính 0 riêng trong component sum.
- Component diff <=0.01 MATCHED; >0.01..0.05 ROUNDING_WARNING; >0.05 REVIEW_WARNING; thiếu thành phần NOT_RECONCILABLE. Mismatch đơn thuần warning, không loại revenue.
- Revenue=reported total positive trên ACCEPTED/WARNING; không dùng calculated total thay reported.
- Recorded-card tip KPI: trusted, payment=1, fare>0, tip>=0; ratio SUM(tip)/SUM(fare) cùng eligible set. Speed KPI: SUM(distance)*3600/SUM(duration) cùng eligible set. Average duration=SUM(duration)/COUNT(eligible duration).
- Không bỏ field natural vendor/payment/rate/location cần phát hiện unmapped. Không chạy spatial join từng trip khi đã có LocationID.
- DUP-001 giữ; DUP-002 historical ngừng phát sinh. Diagnostic candidate in-file không thay disposition. Các rule khác và seed mapping giữ nguyên.

## 5. Physical storage và source-to-target mapping

Tên cột mặc định giữ tên hiện hữu trừ alias có ghi; dictionary này là allowlist cho Fact đích. Không giữ cột chỉ vì model cũ SELECT *.

| Nhóm | Columns/kiểu đích | Nguồn/cách dùng |
|---|---|---|
| Identity/lineage | trip_key bigint; source_record_key char(64); file_version_id bigint; source_row_number bigint; batch_id bigint | trip_key identity; còn lại staging; batch_id là batch gần nhất thực sự ghi row, skip không đổi |
| Date keys | pickup_date_key, dropoff_date_key integer | DimDate lookup timestamp::date |
| Time keys | pickup_time_key, dropoff_time_key smallint | DimTime, minute-of-day +1 theo mapping hiện hữu |
| Other keys | pickup_zone_key, dropoff_zone_key, vendor_key, payment_type_key, rate_code_key integer | Seeds/lookup, unknown=0, N/A=-1 |
| Natural diagnostics | vendor_id, rate_code_id, payment_type_id, pu_location_id, do_location_id smallint | Giữ giá trị canonical unmapped, không ép thành 0 |
| Times | pickup_datetime, dropoff_datetime timestamp; duration_seconds integer | duration trung gian bigint trước quarantine, chỉ narrow khi vào Fact |
| Measures | trip_count smallint; passenger_count smallint; trip_distance_miles numeric(10,3) | trip_distance → trip_distance_miles |
| Money | fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee numeric(12,2) | Giữ tất cả components để giải thích revenue |
| Derived measures | calculated_total_amount, revenue_amount numeric(12,2); row_speed_mph numeric(10,2) | Calculated có thể null, revenue/eligibility theo mục 4 |
| DQ | primary_disposition varchar(24); dq_rule_ids text[]; component_reconciliation_status text | Fact chỉ ACCEPTED/WARNING; dq_severity derive từ rule nếu cần |
| Eligibility | is_revenue_kpi_eligible, is_tip_kpi_eligible, is_speed_kpi_eligible, is_duration_kpi_eligible, is_passenger_kpi_eligible boolean | Không null; measure dùng đúng denominator |
| Source flag | store_and_fwd_flag char(1) | Normalized Y/N/null; giá trị sai điều tra từ nguồn/quarantine |

Không lưu Fact: raw_payload/raw_values, hai business hashes, schema/file checksum/text, reported_total_amount (alias total_amount), row_tip_rate (derive), passenger_band/group, distance_band, pickup/dropoff_time_band, dq_severity, reversal_or_adjustment_candidate, ingested_at/source_last_modified. Bands derive bằng view/BI với cùng edge thresholds. Reversal flag derive từ rules, không mất khả năng phân tích.

Metadata file giữ một lần trong control.source_file_version: source_file, source_year_month, SHA256, schema_version/fingerprint, source_last_modified, source_url/content length; control.etl_file/batch giữ attempts/timestamps/counts. Compatibility view `dds.v_fact_taxi_trip_analysis` join metadata many-to-one và derive các fields bị normalize; không tạo table sao chép text. UI/dashboard phải phân biệt source month với pickup/dropoff month.

Business quarantine: source identity/lineage như trên trừ trip_key, primary_disposition/rules/component status; canonical timestamps, natural codes/location, passenger/distance và monetary components. Không dimension role keys, KPI/band columns, toàn raw JSON hoặc hashes. Duplicate quarantine: cùng canonical diagnostic projection + survivor_source_record_key/occurrence, không giữ JSON/hash persistent. Technical quarantine hiện do Mage sở hữu: giữ nguyên trong Cụm 2, không mất raw malformed values. Có thể normalize riêng sau, không scope-creep.

Workspace classification chứa columns cần cấp Fact/DQ, source original store flag để rule, transient hashes, survivor/occurrence và candidate diagnostic; không raw JSON output. Không cần lưu toàn bộ booleans time_001/amt_... nếu rules array đủ cho consumer. Giữ tuple scope kiểm tra trước build.

Reference dimensions giữ sáu bảng và stable mappings hiện tại; geometry ở bảng/reference zone riêng một lần, map join bằng location_id; 264/265 không cần polygon. Date/Time init một lần; out-of-range date hợp lệ phải mở rộng hoặc fail rõ, không âm thầm gán unknown.

## 6. OLAP và ánh xạ yêu cầu

Ưu tiên giữ ba aggregate hiện có sau sửa phạm vi, bổ sung view trên Fact cho chiều chưa có; không tạo CUBE mọi tổ hợp. Khi cần physical aggregate mới phải chứng minh query dashboard cần nó và đo size.

| Yêu cầu từ tài liệu | Data path/columns | Kiểm chứng |
|---|---|---|
| 5.1 chuyến/hành khách ngày→tháng→năm | daily + DimDate; eligible passenger sum/count | Fact vs daily trip/passenger totals |
| 5.2 revenue borough/vendor ngày/giờ/tháng/năm | Fact analysis view + pickup Date/Time/Zone/Vendor; monthly zone cho source-month audit | Group sum revenue, pickup default, dropoff role truy được |
| 5.3 payment theo zone/tháng | Fact + Payment/Zone/Date; monthly zone chỉ khi đúng month semantics | Count payment groups = trusted trips |
| 5.4 pickup/dropoff giờ/ngày tuần/tháng/zone | Fact + hai role Date/Time/Zone | Hai chiều pickup/dropoff group totals |
| 5.5 nhóm passenger theo rate/distance/time | Analysis view derive group/band + Rate/Date/Time | Boundary 1/2/4/5, invalid, band edges |
| 5.6 dashboard khu vực | Zone + geometry reference + Fact/aggregate | Map/unmapped/non-geometric members riêng |
| 5.7 top N route | monthly route hoặc Fact theo pickup/dropoff zone | Route counts và ranking khớp |
| 5.8 revenue/reversals/missing fee/mismatch | total_amount/revenue/components/DQ + quarantine | Count và amount-eligible reconciliation |
| 6.1 tip/speed theo rate | Fact + Rate; sum tip/fare và distance/duration cùng eligibility | Ratio-of-sums khác average-of-averages |
| 6.2–4 May/June, MoM/YoY, revenue vs trips | Pickup calendar month mặc định; Fact/daily + DimDate | Đủ 24 file, kỳ so sánh đúng, counts và revenue |
| 6.5 map/borough trends/time band | Fact analysis + role views + geometry | Cả pickup/dropoff, hour/band giữ đủ |
| ETL metadata/retry/schedule | Control attempts/file/version/watermark/log | Fault matrix + skip/retry test + evidence automation |

**Daily correctness:** lấy affected pickup dates từ old+new trước mutation, aggregate tất cả Fact trong các dates đó, không chỉ current file. Replacement có group biến mất phải DELETE trước và INSERT groups còn tồn tại; empty current accepted set vẫn dùng old grains đã chụp. Với tháng phân tích lấy pickup/dropoff dates, không thay bằng source_year_month. Monthly source aggregates vẫn được giữ làm audit nếu tên/docs rõ. T07 không được giả định 3 aggregate đủ mọi phân tích.

Mọi average có sums/counts và null/zero denominator xử lý đúng. Không bắt buộc materialize OLAP riêng cho mọi chiều; Fact analysis view là đường truy vấn hợp lệ. T12 kiểm dashboard thực tế riêng.

## 7. Reconciliation/tests/publication

- Source rows = canonical rows + technical rows. Canonical = Fact + business quarantine + duplicate quarantine + intentionally excluded; excluded mặc định 0, nếu phát sinh phải có reason/count, không biến mất.
- Cân fare/tip/total theo từng field với eligible numeric count + sum; technical nonnumeric không được coi zero hợp lệ. Revenue cân với positive trusted totals, không ép source total = revenue khi có rejects.
- Batch tests: scope, schema, source identity, uniqueness/grain, FK, rules/disposition, membership, duplicate survivor/candidate summary, counts/amount. OLAP parity chạy trong job reporting riêng. Không sampling thay kiểm đầy đủ.
- Full audit checkpoint/cuối backfill: 24-file completeness, global unique/FK, total row/amount; OLAP audit ở checkpoint reporting. Không archive/history scan.
- Trình tự runner DDS: scope/skip → workspace/DDS → batch tests/reconciliation → lưu đối soát → atomic metadata DDS SUCCESS + watermark → cleanup. Lỗi trước commit không đổi watermark; cleanup failure chỉ retry cleanup. Sau nghiệm thu DDS, job reporting riêng build/test OLAP rồi mới cho refresh Power BI. Reporting failure không rollback watermark DDS.
- SUCCESS publication tối thiểu: BI không refresh khi batch/backfill pending; query trực tiếp persistent table có thể thấy trạng thái trung gian. Không hứa atomic publication nếu chưa có layer riêng.
- Registry/archive flags legacy không được âm thầm chấp nhận mà bỏ qua: adapter báo unsupported hoặc migration guide rõ. Ref/source/tests liên quan phải được sửa cùng code.

## 8. Retention/index/migration

- Persistent: Fact, gọn DQ, dimensions/geometry, control metadata, aggregate và summaries validation. Immutable source file checksum-verified phải còn truy được để tái tạo; không tự xóa Parquet sau cleanup.
- Transient: raw/canonical/classification, chỉ cleanup khi SUCCESS và writer scope đúng. Không cleanup technical rejects cần audit.
- Chỉ index baseline cần: Fact PK trip_key, unique source key, file_version_id; DQ source key/version; workspace source key unique và version/disposition nếu plan cần; reference natural/role keys. ANALYZE sau bulk build. Index bổ sung phải có evidence.
- T11: với Fact chưa tồn tại, insert vào target mới rồi bulk-build đúng PK/unique/version index trong cùng transaction dưới writer lock, trước post-hooks/commit. Target hiện hữu giữ nguyên constraints và upsert. Đây là thay đổi thứ tự xây index cho initial load, không thay grain, identity, schema hoặc quy tắc retry/replacement.
- Không migration live ở Cụm 1. T09 tạo migration/rollback explicit, kiểm dependency trước bỏ columns. Giữ bảng history/registry cũ read-only trong giai đoạn chuyển đổi; runtime mới không phụ thuộc chúng, cũng không auto-drop chúng.
- Không sửa historical control metadata để giả run mới. Decommission legacy và thu hồi VHDX là scope riêng.

## 9. Parity được phép và không được phép

T11 tối ưu vật lý lookup/window (27/11/2026): nhánh minimal của zone staging đọc các scalar JSON theo metadata cột, giữ nguyên thứ tự tên quoted/lowercase và null fallback. `minimal_validate_scope` đã xác minh duy nhất batch/version/source SHA và source row identity trước classification; vì vậy duplicate window chỉ cần partition theo exact fingerprint trong nhánh minimal. Hai window dùng cùng ROWS frame, giữ survivor theo source_row_number đã kiểm chứng duy nhất. Không đổi hash, rule, threshold, disposition hoặc schema workspace; nhánh legacy giữ nguyên partition/source projection. Phải có strict row parity và kiểm tháng thật trước nghiệm thu hiệu năng.

Cho phép: không còn historical DUP-002/has_cross_file_match, registry events/archive rows; chỉ-source-cross-file warning có thể từ WARNING thành ACCEPTED nhưng counts/measures/eligibility phải không đổi. Candidate in-file summary mới có thể tăng diagnostic counts, không đổi disposition. Physical columns normalize/derive khác, output analysis tương đương. trip_key tái cấp sau reject→reaccept được nêu ở mục 2, không yêu cầu recover từ history.

Không cho phép: thay source identity/exact hash; gộp precision-different raws; mất trip out-of-file month; đổi DQ thresholds, fees/monetary rounding, business key mappings, eligibility, revenue/tip/speed formula, bỏ technical reject hoặc làm mất old OLAP grains. Mọi difference ngoài allowlist là lỗi hoặc phải quay lại sửa contract tường minh trước implement.

T11 sửa planning/raw projection sau R17 (27/11/2026): minimal staging dùng trực tiếp version của canonical đã được scope gate xác minh, bỏ join registry dư theo filename/checksum. Classification lấy writer lock và ANALYZE raw/canonical cùng seed lookups trước validation/planning; ANALYZE workspace sau build. Role vận hành cần quyền ANALYZE trên các relation tương ứng (PostgreSQL16: ownership/role chủ sở hữu phù hợp). Không thay cấu hình memory/threads/durability. Serializer chiếu raw scalar một lần, giữ riêng cờ JSON hiện diện và kiểm cấu trúc; chỉ key/hash/contract boolean đi qua join canonical, không thêm bảng persistent. Giữ nguyên ordinal/type, schema v001/v002, airport fallback và mọi scope checks. Sample parity không thay acceptance toàn tháng.

## 10. Handoff

T02 có fixture JSON và oracle độc lập, expected literals và scenario matrix. Đây là acceptance specification, chưa chứng minh SQL mới PASS. T03–T08 phải chạy output SQL/runner thực với cùng input và so expected; T09 chạy QA recovery. Không nhập oracle vào runtime và không đổi expected để hợp thức hóa implementation lỗi.
