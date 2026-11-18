# Audit và chỉnh sửa dbt Data Warehouse – NYC Yellow Taxi

**Ngày audit:** 2026-11-17
**Phạm vi đã chốt:** Yellow Taxi 2024–2025, các giai đoạn dbt 0–4.
**Nguồn đối chiếu:** `docs/mapping/DBT_HANDOVER_CONTRACT.md`,
`docs/decisions/PHASE2_DESIGN_FREEZE_PROPOSAL.md`,
`docs/etl_design/DBT_IMPLEMENTATION_PLAN.md`, source code và fixture trong
repository. `docs/data_modeling/` hiện chỉ chứa `.gitkeep`; không có báo cáo
Data Modeling riêng để kiểm chứng. Design Freeze là hợp đồng đã duyệt nhưng
không được dùng thay cho kiểm tra mã.

## 1. Trạng thái và inventory ban đầu

- Môi trường cục bộ: `dbt-core 1.8.7`, `dbt-postgres 1.8.2`. Repository chưa
  pin các phiên bản này trong một dependency file.
- Manifest khi bật `fixture_mode`: 17 models, 6 seeds, 9 sources và 157 tests
  trước audit. Sau khi bổ sung một test ánh xạ FK: 158 tests theo `dbt parse`.
- Staging/intermediate là views; sáu dimensions và hai business DQ branches là
  tables; Fact dùng materialization PostgreSQL `fact_identity`; bảng đối soát
  file là table. OLAP models chưa có, đúng với trạng thái giai đoạn 5.
- `trip_key` được tạo tại bảng đích bằng `bigint generated always as identity`.
  `source_record_key` là unique/not-null và điều kiện upsert. Hai fingerprint
  không được dùng làm surrogate key hoặc unique key của Fact.
- Không có database mẫu đang hoạt động tại thời điểm audit. Không tạo, xóa
  hoặc sửa database để thực hiện nhiệm vụ này. Các kết quả 180/180 và 157 test
  trước đây là lịch sử, **không phải kết quả kiểm thử của lần audit này**.

## 2. Bảng phát hiện và quyết định

| File/khu vực | Nội dung | Phát hiện và bằng chứng | Mức độ | Quyết định |
|---|---|---|---|---|
| `macros/fact_identity_materialization.sql` | Identity và upsert | Tạo `trip_key` bằng identity trong bảng đích; `ON CONFLICT (source_record_key)` cập nhật các cột khác, giữ `trip_key`. Không dùng `ROW_NUMBER()` hay fingerprint làm khóa. | Đúng | Giữ cơ chế. |
| `macros/fact_identity_materialization.sql` và `tests/singular/fact_accepted_membership.sql` | Đổi trạng thái Fact | Trước audit, bản ghi đã ở Fact nhưng nay chuyển sang quarantine vẫn nằm trong Fact; test hậu kiểm phát hiện nhưng một số upsert khác có thể đã commit. | HIGH | Thêm preflight chặn upsert khi có khóa Fact không còn accepted. Chưa tự động xóa; cần quy trình archive và correction có kiểm soát. |
| `models/intermediate/int_taxi_trip_enriched.sql` | Duration | `extract(epoch ...)::integer` chạy trước khi phân loại thời gian. Hai timestamp cách nhau hơn giới hạn `integer` có thể làm build lỗi dù dòng đó lẽ ra vào quarantine. | HIGH | Tính trung gian bằng `bigint`; Fact vẫn cast về `integer` sau khi đã qua DQ. |
| `models/marts/facts/fact_taxi_trip.sql` và `tests/singular/dq_policy_configuration.sql` | Kiểu duration Fact | Fact giữ tên `duration_seconds` và kiểu `integer` theo hợp đồng hiện tại; ngưỡng quarantine có thể cấu hình. | MEDIUM | Chặn cấu hình ngưỡng vượt miền `integer`; không đổi schema Fact. |
| `tests/singular/fact_accepted_membership.sql` | Bảo toàn measure/lineage | Trước audit chỉ so fingerprint, disposition, revenue và cờ KPI; sai lệch fare, distance, duration hoặc lineage có thể không bị test này bắt trực tiếp. | MEDIUM | Mở rộng so sánh null-safe các measure nguyên tử, thời gian, mã nguồn, `trip_count` và lineage. |
| `tests/singular/fact_dimension_key_mapping.sql` | FK hợp lệ nhưng sai member | `relationships` bắt orphan FK, nhưng một FK trỏ sang member khác vẫn có thể hợp lệ về mặt tham chiếu. | MEDIUM | Thêm test so surrogate với lookup từ natural code cho Zone/Vendor/Payment/Rate. |
| `models/intermediate/int_taxi_trip_classified.sql`, `seeds/dq_rule_policy.csv` | DQ routing | Null/0 passenger và zero distance là warning; Zone 264/265 là business member; money âm/void/negative duration vào business quarantine theo Design Freeze; mismatch component chỉ warning. | Đúng theo hợp đồng | Giữ. Không suy mọi dòng warning đều không dùng được cho tất cả KPI. |
| `models/quality/business_quarantine.sql`, `duplicate_quarantine.sql` | Truy vết | Hai nhánh giữ `source_record_key`, file, batch, reason (`dq_rule_ids`) và nguồn raw qua khóa. Same-file raw-exact loser vào duplicate; cross-file match chỉ warning. | Đúng theo hợp đồng | Giữ. Nội dung giống nhau vẫn có thể là hai chuyến khác nhau; không có trip ID nguồn để khẳng định ngược lại. |
| `models/quality/file_reconciliation.sql` | Đối soát | Có phương trình row/amount theo file. `source_record_key` unique tests làm distinct count bằng row count khi tests đạt. Mô hình hiện không có số lượt replay/skipped và không chia theo batch vì giao diện technical/raw chưa cung cấp metadata đầy đủ. | NOT VERIFIED | Giữ công thức đã duyệt; cần Mage cung cấp batch/replay metadata trước khi mở rộng. |
| `models/sources/taxi_sources.yml` | Technical reject | Giao diện tạm chỉ có key, file SHA/tên và raw amount text; thiếu reason, batch, source row number. dbt không thể kiểm lỗi Parquet trước khi file vào database. | NOT VERIFIED | Chỉ đánh giá contract; không triển khai Mage hoặc giả lập khả năng ngoài SQL. |
| `models/marts/dimensions/`, `models/marts/facts/` | Dimensional model | Một Fact row cho một accepted/warning source record; sáu physical dimensions; role keys pickup/dropoff; key 0 Unknown, -1 N/A; code 0 Payment và Zone 264/265 có surrogate dương. | Đúng qua mã; DDL thật NOT VERIFIED | Giữ tên và grain. |
| `scripts/build_with_detailed_tests.py` | Test reporting | Dùng `--store-failures`, đọc `run_results.json`, in từng test và vài dòng lỗi; trả về exit code của dbt. | Đúng qua mã | Giữ. Chưa chạy lại trong audit này. |

## 3. Fact schema, KPI và ngữ nghĩa

- Fact có `trip_key bigint identity`, các FK Date/Zone/Vendor/Payment/Rate
  dạng integer và Time dạng smallint, `trip_count smallint = 1`,
  `passenger_count smallint nullable`, `trip_distance_miles numeric(10,3)`
  theo canonical, `duration_seconds integer nullable` cho dòng accepted,
  các khoản tiền `numeric(12,2)` theo hợp đồng. Đây là type từ biểu thức SQL
  và hợp đồng canonical; physical schema Mage/Fact hiện hành **NOT VERIFIED**.
- Pickup/dropoff là timestamp không timezone, giữ giờ địa phương New York.
  `cbd_congestion_fee` thiếu về cấu trúc ở 2024 được giữ NULL; chỉ dùng 0 khi
  cộng component cho phiên bản nguồn thiếu cột. NULL quan sát được ở 2025
  không bị ép thành 0.
- `total_amount` là tổng tiền nguồn ghi nhận; `revenue_amount` là positive
  reported total của accepted/warning. Không diễn giải nó thành net revenue.
  `SUM(passenger_count)` là lượt hành khách được ghi nhận, không phải số
  hành khách duy nhất.
- Các cờ KPI độc lập. Speed tổng hợp và tip rate tổng hợp chưa có OLAP model;
  khi triển khai phải dùng cùng tập eligible cho tử/mẫu và ratio-of-sums,
  lần lượt `SUM(distance)*3600/SUM(duration_seconds)` và
  `SUM(tip_amount)/SUM(fare_amount)`. Cash tips không đầy đủ trong nguồn.
- Không có mapping legacy surcharge sang `extra` trong phạm vi 2024–2025.

## 4. Trước/sau các thay đổi

1. **Duration outlier:** trước `extract(epoch)::integer` ở intermediate; sau
   `::bigint` ở intermediate, `::integer` tại Fact sau lọc DQ. Kiểm tra cấu
   hình buộc ngưỡng quarantine nằm trong miền `integer`.
2. **Accepted → rejected:** trước chỉ có test phát hiện Fact stale sau khi
   materialization chạy; sau materialization kiểm tra một key stale và dừng
   trước upsert. Không tự xóa Fact, Raw, staging hoặc quarantine.
3. **Độ phủ test Fact:** trước test partition chỉ so một phần trạng thái và KPI;
   sau so thêm measure, duration, `trip_count`, lineage và FK business mapping.

## 5. Kiểm tra idempotency, quarantine và reconciliation

- **Idempotency theo thiết kế:** physical unique trên `source_record_key`,
  `ON CONFLICT` cập nhật mà không thay `trip_key`; unchanged row được bỏ qua.
  Rerun và concurrent build trong lần audit này **NOT VERIFIED** vì không có
  database kiểm thử. Lần build đầu đồng thời vẫn có thể race khi tạo bảng;
  không nên chạy hai khởi tạo cùng lúc.
- **Quarantine:** các nhánh accepted, business và duplicate xuất phát cùng
  classified view; `trip_classification_partition` kiểm đúng một nhánh cho mỗi
  canonical key. Technical rejects do Mage sở hữu. Runtime routing sau khi
  đổi chính sách **NOT VERIFIED**.
- **Reprocessing:** rejected → accepted có thể insert theo source key;
  warning → accepted có thể upsert giữ `trip_key`; accepted → rejected bị
  preflight chặn để tránh Fact stale bị lẫn vào run mới. Bước archive và xóa
  Fact có kiểm soát chưa được triển khai/test vì thiếu database mẫu và hợp đồng
  audit vận hành. Cần kiểm tra raw, reason, batch và lưu bản Fact trước khi
  thực hiện correction trong một transaction có review.
- **Đối soát:** phương trình theo file là
  `raw = canonical + technical` và
  `canonical = Fact + business + duplicate`; amount chỉ gồm dòng cast được
  `numeric(12,2)` với dung sai cấu hình USD 0.01/file. Không cộng số lỗi DQ
  như các nhánh riêng. Replayed/skipped record và đối soát theo batch/thời kỳ
  **NOT VERIFIED** vì metadata nguồn hiện chưa đủ.
- **Watermark:** chỉ có source contract và hướng dẫn Mage commit sau dbt;
  chưa kiểm tra orchestration hoặc tình huống lỗi thực tế.

## 6. Đánh giá độ phủ tests

| Tình huống | Bằng chứng test hiện có / giới hạn |
|---|---|
| Duplicate source key, null/duplicate `trip_key` | Generic source/Fact tests và physical unique/PK; runtime audit chưa chạy. |
| Broken FK và wrong member | Generic `relationships`, `fact_date_time_role_keys`, test mới `fact_dimension_key_mapping`. |
| Invalid routing, accepted/rejected overlap | `trip_classification_partition`, `fact_accepted_membership`; preflight mới chặn Fact stale trước upsert. |
| Missing records và money mismatch | `file_reconciliation_balances`, source key test; chỉ theo file. |
| Incorrect speed denominator | Row speed được tính từ distance/duration; aggregate speed chưa tồn tại, thuộc giai đoạn 5. |
| Reprocessing/identity stability | Code upsert và test identity hiện có; chuyển trạng thái và chạy lại chưa test trong audit này. |
| Unknown/NA member | Dimension/member tests, Fact role/member tests. |
| Sai schema-version registry | Có test hiện diện cột và CBD theo năm; mapping fingerprint → version thực do Mage quản lý, NOT VERIFIED. |

## 7. Validation của lần audit này

| Tầng kiểm tra | Lệnh/kết quả |
|---|---|
| Static dependencies | `dbt deps`: exit 0; project không có `packages.yml` để cài. |
| Static parse | `dbt parse --vars '{fixture_mode: true}'`: exit 0; manifest 17 models, 6 seeds, 9 sources, 158 tests. Có warning cấu hình `marts.olap` chưa áp dụng vì giai đoạn 5 chưa xây. |
| Compilation | `dbt compile --no-introspect` đã thử nhưng exit 2 do PostgreSQL tại `127.0.0.1:55438` không hoạt động. SQL sau sửa chưa được compile với adapter; **NOT VERIFIED**. |
| Test database | Không có database mẫu; không chạy `dbt build`, `dbt test`, initial build, rerun, incremental insert/update hay negative tests trong lần audit này. |
| Real data | Chưa chạy. |

## 8. File thay đổi và công việc tiếp theo

Đã sửa: `models/intermediate/int_taxi_trip_enriched.sql`,
`models/marts/facts/fact_taxi_trip.sql`,
`macros/fact_identity_materialization.sql`,
`tests/singular/dq_policy_configuration.sql`,
`tests/singular/fact_accepted_membership.sql`, `README.md`.
Đã thêm: `tests/singular/fact_dimension_key_mapping.sql` và báo cáo này.
Không tạo commit/push, không full-refresh và không đụng database thật.

Trước khi xem các sửa đổi là production-ready, cần một database mẫu được cấp
phép để chạy compile/build, các chuyển trạng thái reprocessing, identity và
reconciliation; đồng thời đo chi phí preflight trên dữ liệu đại diện. Giai
đoạn 5 còn các OLAP models và kiểm thử ratio-of-sums. Giai đoạn 6 còn DDL Mage,
PostGIS/SRID, registry schema fingerprint, technical reason/batch metadata,
replay counters, watermark và dữ liệu thật.

## 9. Bổ sung sau audit: Fact lifecycle và kiểm chứng runtime (17/11/2026)

Các dòng **NOT VERIFIED** ở mục 1–8 ghi đúng trạng thái *tại thời điểm audit ban đầu*;
chúng không mô tả kết quả của đợt kiểm chứng tiếp theo. Chi tiết thiết kế, lệnh,
kịch bản, số liệu và giới hạn nằm trong
[FACT_LIFECYCLE_RUNTIME_VALIDATION.md](FACT_LIFECYCLE_RUNTIME_VALIDATION.md).

- Đã bổ sung chế độ `fact_lifecycle_mode: archive` để ghi lịch sử và loại Fact row
  không còn thuộc `ACCEPTED/WARNING`; mặc định `guard` tiếp tục chặn trường hợp
  này nếu chưa quyết định migration. Lịch sử lưu các chuyển trạng thái, ảnh Fact
  trước thay đổi và mapping `source_record_key → trip_key` để phục hồi identity.
- Đã chạy `dbt deps`, `dbt parse`, `dbt compile`, `dbt build`, `dbt test` trên
  PostgreSQL 18 với fixture cô lập. Full build cuối: **PASS 182, WARN 0,
  ERROR 0, SKIP 0**; `dbt test`: **PASS 159, WARN 0, ERROR 0, SKIP 0**.
- Đã kiểm chứng rerun, insert/update batch mới, các chuyển trạng thái
  ACCEPTED/WARNING/REJECTED/DUPLICATE, key stability, migration từ Fact cũ,
  hai writer đồng thời, rollback bằng lỗi có chủ đích, và các negative tests
  cho lịch sử và amount reconciliation.
- Đối soát cuối có 4 file mẫu; mọi chênh lệch row và amount theo file bằng 0.
  Chưa có metadata đủ để đối soát replay/skipped hay phân tách theo batch.
- Hai database mẫu được tạo riêng cho đợt này đã xóa và xác nhận không còn.
  Chưa kiểm chứng trên dữ liệu thật, Mage/PostGIS, watermark hoặc hiệu năng
  ở quy mô đầy đủ. Không có commit/push Git.

**Điều chỉnh sau báo cáo (17/11/2026):** Theo yêu cầu người dùng, 2024–2025
chỉ là phạm vi file team chọn nạp. Đã bỏ rule `TIME-006` và test chặn pickup
ngoài khoảng năm này; `TIME-005` chỉ còn cảnh báo tháng pickup khác tháng file.
Các số liệu runtime cũ trong báo cáo liên kết ở trên được đo trước điều chỉnh
này. Sau sửa, full `dbt build` chế độ `guard` trên fixture cô lập đạt
**PASS 181, WARN 0, ERROR 0, SKIP 0**. Một dòng mẫu thêm vào file 2025-05 với
pickup 2026-05 nhận duy nhất `TIME-005`, ở trạng thái `WARNING` và vẫn vào Fact;
đối soát theo file có 0 chênh lệch. Database mẫu mới đã xóa sau kiểm chứng.

## 10. Đối chiếu handover contract và dọn dbt project (17/11/2026)

Phạm vi rà soát tên/xóa file chỉ trong `dbt_project`, theo xác nhận của người
dùng. Phần lõi dbt **phù hợp với contract trên dữ liệu mẫu**, nhưng chưa thể
kết luận toàn bộ luồng Mage/PostGIS và dữ liệu thật đạt contract.

| Mục hợp đồng | Kết quả hiện tại |
|---|---|
| Canonical → staging → enriched/fingerprinted/classified | Có model, phân loại theo policy seed, raw/candidate fingerprint và các test tương ứng; fixture chạy đạt. |
| Technical reject, file/schema checks | Thuộc Mage; dbt chỉ đọc giao diện technical tạm. Chưa có DDL/pipeline Mage thật để kiểm tra. |
| Same-file duplicate và cross-file warning | Có model, kiểm tra winner/retention và phân nhánh trên mẫu. |
| Sáu dimensions, Fact identity, key 0/-1, KPI flags | Có model, physical PK/unique Fact và dbt tests. Đã thêm test `canonical.batch_id → control.etl_batch.batch_id`. |
| Row/amount reconciliation | Đạt theo file trên fixture; thiếu metadata replay/skipped và kiểm tra theo batch ở giao diện nguồn. |
| Geometry/Zone | Lookup/geometry membership và số lượng được test trên mẫu; SRID chưa test do thiếu tên cột/DDL PostGIS thật. |
| Schema registry/Raw precision | `schema_fingerprint` có trong contract và hash; liên kết fingerprint → `schema_version` do Mage quản lý chưa được kiểm chứng. CBD structural absence hiện suy từ `source_year_month`, phù hợp hai schema mẫu nhưng chưa đủ bằng chứng cho schema version khác. |
| P99 monitoring và KPI aggregate | Chưa có bảng monitoring P99; các công thức aggregate tip/speed thuộc OLAP giai đoạn kế tiếp. |

`duration_seconds` ở intermediate dùng `bigint` để phân loại timestamp lỗi
trước khi ép sang `integer` trong Fact. Đây là khác biệt có chủ đích so với
kiểu `integer` ghi cho intermediate trong handover contract; nên cập nhật mô
tả contract khi duyệt thay đổi tài liệu, không thu hẹp phép tính trung gian.

Đã đổi tên **28 singular tests** theo nội dung (ví dụ
`fact_accepted_membership.sql`, `fact_lifecycle_history.sql`,
`file_reconciliation_balances.sql`), cập nhật tham chiếu tài liệu và bỏ tag
`phase3` của Fact. Đã xóa **12 `.gitkeep`** không còn giá trị; bỏ khai báo đường
dẫn analyses/snapshots chưa dùng, cấu hình OLAP chưa có model, khai báo
`incremental` bị materialization Fact ghi đè, và biến SRID chưa được thực thi.
Giữ README, profile mẫu, script báo test chi tiết, fixture seed
và hai báo cáo vì chúng phục vụ vận hành/kiểm chứng, không phải file dư.

Trên database mẫu cô lập mới, `dbt build` với `fixture_mode: true` và
`fact_lifecycle_mode: archive` đạt **PASS 183, WARN 0, ERROR 0, SKIP 0**
(17 models, 6 seeds, 160 tests); Fact 9.363 row, đối soát file không có sai
lệch. Không còn cảnh báo đường dẫn cấu hình OLAP chưa dùng. Database mẫu đã
được xóa và truy vấn xác nhận còn 0 database trùng tên; không commit/push Git.

Sau full build, test mapping seed được chỉnh để lấy bộ mã kỳ vọng từ
`dbt_project.yml`, dùng số Zone cấu hình và không ép công thức surrogate
`location_id + 1000`. Trên database seed cô lập, `dbt seed` và
`dbt test --select reference_seed_mapping_contract` đều exit 0. Cố ý đổi
`expected_vendor_codes` sai khiến test exit 1; chạy lại cấu hình mặc định
exit 0. Database seed mẫu cũng đã xóa, xác nhận còn 0 database trùng tên.
Full build 183/183 ở trên được chạy trước chỉnh sửa cuối của test mapping;
phần SQL thay đổi đã được kiểm riêng bằng test dương và âm này.
