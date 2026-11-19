# Kế hoạch task: tối giản dbt cho đồ án BI

Ngày lập: 2026-11-19. Trạng thái: **Cụm 1–2 (T00–T05) DONE theo phạm vi; DDS opt-in đã kiểm QA, chưa chuyển live**.

## 1. Mục tiêu và cách sử dụng

Giảm tổng thời gian dbt, thời gian chạy lại và dung lượng đỉnh/giữ lâu dài, nhưng vẫn chứng minh đầy đủ ETL và phân tích theo yêu cầu đồ án. Không lấy số lượng model/test hay mức phức tạp production làm mục tiêu.

Agent nhận một task phải đọc mục 1–6, task được giao, task phụ thuộc và cập nhật tiến độ trước khi làm. Chỉ triển khai task được giao và phần phụ thuộc nhỏ cần thiết; không tự triển khai toàn bộ kế hoạch. Kế hoạch này không yêu cầu tạo subagent hoặc chat mới.

Đầu vào:

- Yêu cầu do người dùng cung cấp: `D:/BI/DATH/Yeu_cau_do_an_va_noi_dung_3_buoi_seminar.md`.
- Báo cáo rà soát: `reports/DBT_PERFORMANCE_BRANCH_REVIEW_2026-11-19.md`.
- Baseline code đã rà: `feature/control-metadata-watermark` tại `c679360`.
- Nguồn tham khảo tối ưu: `origin/feat/fix-dbt` tại `5fcf661`.

Tài liệu yêu cầu là bản tổng hợp đề bài, thiết kế nhóm và seminar. Phải phân biệt yêu cầu học phần với quyết định thiết kế cũ của nhóm và ví dụ trong seminar. Không suy diễn mọi cơ chế production trong repo là bắt buộc của môn học. Chưa đối chiếu lại PDF/video gốc trong kế hoạch này; không tuyên bố đã xác minh trực tiếp các nguồn đó.

Đây là đề xuất thay đổi thiết kế theo yêu cầu mới của người dùng, không phải xác nhận đã migrate database. Nếu tài liệu cũ mâu thuẫn, chỉ rõ mục nào bị thay thế bởi thiết kế tối giản, cập nhật contract tương ứng; không giả tạo sign-off và không giữ gate yêu cầu archive cho một tính năng đã được loại bỏ khỏi luồng mới.

## 2. Những yêu cầu bắt buộc giữ

1. Xử lý đủ hai năm đã chọn 2024–2025, tương ứng 24 file tháng theo catalog. Không đổi thành hai tháng hoặc lấy sample để tuyên bố hoàn thành.
2. Ba nguồn: trip records, zone lookup, shapefile. Giữ zone geometry dùng cho bản đồ; không spatial join từng trip nếu LocationID đã cung cấp đủ mapping.
3. Kiến trúc Source → Staging → DDS → OLAP/Power BI; dữ liệu dashboard đến từ pipeline.
4. Fact grain một chuyến được chấp nhận; sáu dimension Date, Time, Zone, Vendor, PaymentType, RateCode và các role pickup/dropoff cần thiết.
5. Schema mapping 2024/2025, lineage tối thiểu, DQ flags và giá trị nguồn cần điều tra cast lỗi.
6. DQ thời gian, distance, passenger, location, amount, duplicate candidate; quarantine có lý do rõ ràng.
7. Kiểm tra khóa, grain, orphan FK, counts và tiền; raw/trusted/reject/excluded đối soát được.
8. Batch/file/checksum/status/log/watermark; retry được, không nạp trùng. Watermark chỉ commit sau các bước bắt buộc thành công.
9. Trigger/schedule và bằng chứng chạy tự động; chạy lại end-to-end ít nhất hai lần.
10. Dữ liệu đủ cho toàn bộ yêu cầu phân tích cơ bản; không cắt mất khả năng tính tip/speed, MoM/YoY hoặc bản đồ chỉ để giảm vài cột.

## 3. Thiết kế đích đã chọn cho kế hoạch

```text
Immutable Parquet + file metadata/checksum
  → Mage: staging một file/version tại một thời điểm
  → dbt staging/enrichment nhẹ
  → một classification table gọn, chỉ chứa dữ liệu đang xử lý
  → Fact hiện tại + quarantine gọn
  → batch tests + reconciliation
  → cập nhật đúng phạm vi OLAP + OLAP tests
  → SUCCESS/watermark
  → cleanup staging/workspace
```

### Những tính năng loại khỏi luồng mới

- Không duy trì registry fingerprint riêng cho mọi chuyến.
- Không duy trì Fact lifecycle event/archive/prior_fact JSONB trong pipeline mới.
- Không dò mọi fingerprint với toàn bộ lịch sử ở mỗi batch; không lưu mọi cặp cross-file match; không cập nhật hồi tố warning của tháng cũ.
- Không chạy lại toàn bộ historical audit ở mỗi tháng.
- Không lưu hai workspace rộng trùng JSON nếu một classification table đủ.

“Loại khỏi luồng” nghĩa là ngừng dependency/ghi/đọc trong implementation mới. **Không đồng nghĩa được xóa bảng hoặc dữ liệu cũ ngay lập tức.** Giữ legacy hiện trạng cho đến migration/decommission được người dùng cho phép.

### Quyết định vận hành mặc định

- Chỉ một writer; scope là batch + file_version_id xác định, staging không lẫn file khác.
- SUCCESS + checksum/version không đổi: skip trước khi gọi dbt; nếu cleanup còn pending thì chỉ retry cleanup, không bỏ qua pending đó.
- File mới: insert Fact và DQ của file đang nạp.
- Retry: thực hiện lại cùng version có kiểm soát; không phụ thuộc workspace cũ để skip bước.
- Checksum đổi: ghi version mới; chỉ thay thế file cũ khi có request replacement tường minh. Không tự suy checksum đổi thành quyền xóa dữ liệu cũ.
- Fact dùng materialization nạp theo file đơn giản. Giữ bigint `trip_key` và unique source key. Có thể delete/insert trong transaction cho retry nếu T01 xác nhận không có consumer phụ thuộc trip_key; nếu có thì giữ upsert đơn giản để bảo toàn key. Không tự bỏ trip_key hay đổi grain.
- Candidate duplicate chỉ kiểm trong file đang xử lý. Candidate không chứng minh duplicate thì chỉ flag; không xóa chuyến khác file do hash giống nhau. Bỏ historical detection phải được mô tả trung thực là giới hạn của policy.
- Giữ công thức revenue, eligible trips, rule thresholds và schema mapping hiện tại trừ thay đổi thiết kế được chỉ định rõ. Mục tiêu là giảm overhead, không đổi số liệu nghiệp vụ.

## 4. Giới hạn áp dụng cho mọi task

### Được làm trong task được giao

- Đọc repo, logs, artifacts; sửa code/tài liệu/test; tạo fixture nhỏ, có giới hạn trong database QA được xác định rõ.
- Chuẩn bị migration và rollback có thể review; parse/compile theo môi trường sẵn có.
- Loại test/model/dependency không còn chức năng tương ứng, đồng thời cập nhật runner, source declarations, docs và fixtures liên quan.
- Chạy kiểm chứng phù hợp thay đổi, ghi rõ cái gì đã/chưa chạy.

### Không được làm

- Không reset database, `docker compose down -v`, xóa volume/VHDX, drop/truncate dữ liệu persistent cũ hoặc đổi trạng thái batch/watermark bằng tay.
- Không chạy migration live, backfill 24 tháng hoặc xóa legacy chỉ vì task code đã hoàn thành. Kế hoạch này là chỉ dẫn implementation; các hành động dữ liệu live phải nằm trong phạm vi người dùng đã cho phép ở lượt triển khai.
- Không sửa Mage ingestion/validation đã đạt yêu cầu ngoài integration cần thiết cho scope, skip, runner, cleanup và metadata. Không đổi nguồn/hai năm để làm benchmark đẹp.
- Không bỏ reconciliation, quarantine, duplicate control hoặc tests bắt buộc; không commit watermark trước thành công.
- Không đổi source key/hash serialization/rule/revenue âm thầm. Không “sửa” expected output để che sai dữ liệu.
- Không dùng `--full-refresh` cho Fact hiện hữu; không dùng `--resume-dbt-select` để bỏ upstream chưa chứng minh thuộc đúng batch/version.
- Không tăng RAM/work_mem/threads toàn cục hoặc tắt durability để che query chậm. UNLOGGED không thuộc thiết kế mặc định; chỉ xem xét workspace tái tạo được sau benchmark riêng.
- Không commit `.env`, password, profile có secret, dữ liệu trip lớn hoặc artifact có payload nhạy cảm.
- Không coi parse PASS, fixture PASS hoặc mẫu nhỏ PASS là bằng chứng chạy đủ một tháng/hai năm.
- Không sửa/xóa thay đổi của người dùng trong working tree; không switch/reset nhánh để so sánh code. Không merge/cherry-pick toàn bộ nhánh fix một cách máy móc.

Nếu thiếu thông tin: tiếp tục phần độc lập; ghi rõ điều chưa biết và hỏi khi nó ảnh hưởng correctness/dữ liệu live. Không tự dựng con số benchmark hoặc yêu cầu duyệt chung chung cho sửa code có thể review.

## 5. Quy tắc trạng thái và bàn giao

Trạng thái task: TODO → IN_PROGRESS → DONE hoặc BLOCKED. DONE chỉ khi đủ deliverable và acceptance; nếu chỉ code xong nhưng database test chưa chạy, ghi CODE_READY và nêu phần còn thiếu. Benchmark task không được DONE khi chưa có phép đo thực.

Mỗi task cập nhật một mục trong `reports/dbt_minimal_refactor/Txx.md` gồm:

1. Commit/branch đầu vào, task dependencies, working-tree changes đã có.
2. Files thay đổi và quyết định kỹ thuật.
3. Tests/lệnh đã chạy, kết quả và nơi lưu evidence; chỉ rõ QA hay live.
4. Counts/amounts/parity cần thiết; limitations hoặc lỗi còn lại.
5. Hướng dẫn task sau và rollback code/schema nếu áp dụng.

Không cần tạo đủ report rỗng từ đầu. Chỉ tạo khi thực hiện task. Khi đổi contract, cập nhật kế hoạch và ghi lý do; không tự mở rộng phạm vi.

## 6. Danh sách và phụ thuộc

| Task | Nội dung                                          | Phụ thuộc | Trạng thái |
| ---- | ------------------------------------------------- | --------- | ---------- |
| T00  | Inventory và baseline                             | Không     | DONE       |
| T01  | Chốt contracts tối giản, mapping yêu cầu          | T00       | DONE       |
| T02  | Fixtures và parity oracle                         | T01       | DONE       |
| T03  | Classification workspace gọn, duplicate theo file | T02       | DONE       |
| T04  | Dimensions tĩnh, lookup keys                      | T01, T02  | DONE       |
| T05  | Fact/DQ nạp theo file, bỏ lifecycle runtime       | T03, T04  | DONE       |
| T06  | Tests theo batch và reconciliation                | T05       | TODO       |
| T07  | OLAP gọn, đúng affected grains                    | T05, T06  | TODO       |
| T08  | Runner skip/retry/cleanup, artifacts              | T06, T07  | TODO       |
| T09  | Migration QA và end-to-end fault/retry            | T08       | TODO       |
| T10  | Benchmark thời gian và dung lượng                 | T09       | TODO       |
| T11  | Tuning theo evidence, chốt phiên bản              | T10       | TODO       |
| T12  | Runbook, full audit và bàn giao                   | T11       | TODO       |

## T00 — Inventory và baseline có thể so sánh

**Mục đích:** biết code/database/consumer đang thực sự có gì trước khi cắt.

**Cần làm:**

- Đọc instructions áp dụng trong workspace, git status và commit. Kiểm tra lại remote ref; báo nếu baseline khác `c679360`/`5fcf661`.
- Inventory models/macros/tests/sources/seeds, runner, SQL migration, Power BI/OLAP references. Tìm mọi consumer của trip_key, raw/candidate hash, lifecycle history, registry và lineage text.
- Đọc hai materialization table và temporary index/ANALYZE từ feat/fix-dbt; chọn phần hữu ích thay vì nhập toàn commit.
- Nếu DB có sẵn và được phép đọc: ghi table/index/constraint, approximate rows, pg_total_relation_size, database size, PostgreSQL settings, disk host/VHDX và trạng thái batch. Không bật Docker hoặc chạy job nặng chỉ để lấy baseline nếu chưa thuộc scope.
- Giữ snapshot evidence đầu vào và phân biệt fixture với dữ liệu thật; run_results mặc định có thể chỉ là seed/OLAP cuối.

**Không làm:** migrate, cleanup, sửa dữ liệu, khởi chạy full-month baseline chậm trên live.

**Nghiệm thu:** report có dependency inventory, candidate bỏ/giữ và danh sách consumer cần sửa; có baseline thực hoặc ghi rõ thiếu. Xác định trip_key có thể đổi khi retry hay phải ổn định.

## T01 — Chốt contracts tối giản và ánh xạ yêu cầu

**Files chính:** tài liệu thiết kế mới, data dictionary, source-to-target mapping; tham chiếu `dbt_project/models/**`, runner và control schema.

**Cần làm:**

- Tạo `docs/etl_design/DBT_MINIMAL_CONTRACT.md`: grain, khóa, file/version scope, duplicate policy, DQ, revenue, retention, publication, retry/replacement.
- Lập bảng mỗi yêu cầu phân tích → columns/dimensions/OLAP hỗ trợ → test chứng minh. Phân biệt source_year_month với tháng pickup/dropoff; không loại trip lệch tháng file.
- Chốt source key: ưu tiên giữ thuật toán hiện tại để giảm migration. File checksum/version và source_row_number phải đủ truy vết; không thay source key bằng business fingerprint.
- Chốt Fact columns: các role keys, pickup/dropoff timestamp, passenger, distance/duration, monetary components, eligibility/DQ, source key, trip_key, file_version_id và source_row_number. Batch lineage tối thiểu trực tiếp hoặc qua metadata; không mất khả năng truy batch khi retry.
- Chốt cột metadata file lưu một lần: filename, checksum, schema version/fingerprint, source month, source timestamps. Compatibility view chỉ khi consumer cần, tránh nhân bản text trên mọi fact row.
- Chốt quarantine giữ source identity + reasons + giá trị cần điều tra; technical reject cần raw values cast lỗi. Bản Parquet bất biến là nguồn tái tạo; retention không xóa file đang cần để retry.
- Chốt loại bỏ legacy DUP-002 historical behavior: loại rule/test/dependency liên quan và ghi tác động lên DQ parity. Không bắt parity với một feature đã chủ động bỏ.
- Chốt lựa chọn Fact insert/delete+insert/upsert dựa T00. Không để agent T05 tự chọn lại tùy tiện.

**Không làm:** thay rule thresholds, định nghĩa revenue/tip/speed hoặc loại 15% nâng cao âm thầm. Các chỗ yêu cầu và thiết kế cũ chưa rõ phải ghi assumption cụ thể.

**Nghiệm thu:** có contract đủ để T03–T08 implement không cần đoán; mọi cột bị bỏ có lý do và cách thay thế; mọi khác biệt dữ liệu được phép đều được liệt kê.

## T02 — Fixtures và chuẩn đối chiếu

**Cần làm:**

- Dùng fixtures nhỏ 2024/2025 hiện có, bổ sung nếu thiếu: CBD fee schema, null/invalid cast, timestamp/amount/location, passenger, exact duplicate trong file và candidate-only duplicate.
- Có hai file chứa cùng fingerprint: policy mới không tự xóa/cập nhật hồi tố chuyến ở file khác. Có trip pickup/dropoff khác tháng file và cùng một ngày pickup xuất hiện ở hai file.
- Có retry cùng checksum, file rỗng/all rejected, version replacement làm một OLAP group biến mất và lỗi giữa các phase.
- Lưu expected keys, counts, fare/tip/total/revenue/eligible numerator-denominator. Dùng baseline code đọc-only hoặc phép tính độc lập, không tạo expected bằng chính SQL mới rồi tự so với nó.
- Parity split: phần nghiệp vụ giữ nguyên phải khớp; historical-warning/archive bị bỏ có expected delta được giải thích.

**Không làm:** thêm test chỉ kiểm chuỗi code chứa từ khóa thay cho kiểm hành vi; chạy fixture vào production.

**Nghiệm thu:** fixtures tái tạo được, có negative case và expected results độc lập cho phần cốt lõi.

## T03 — Một classification workspace gọn

**Files chính:** `dbt_project/models/staging/stg_yellow_taxi.sql`, `models/intermediate/*`, `macros/fingerprint_fields.sql`, `dbt_project.yml`, audit sources/models liên quan.

**Cần làm:**

- Giữ staging/enrichment nhẹ dạng view; materialize classification thành table dựng lại mỗi file. Truyền batch/version tường minh; từ chối scope trộn nếu không hỗ trợ.
- Gộp bước fingerprint chỉ dùng một lần trong SQL classification; chỉ giữ một physical workspace mặc định. Không dùng ephemeral như một cache giữa nhiều query.
- Projection cột rõ ràng, bỏ raw JSON khỏi output classification. Nếu fingerprint còn cần JSON, chỉ đọc trong query tính hash hoặc nhánh contract check riêng.
- Giữ in-file duplicate survivor xác định theo source_row_number; kiểm policy khi null hash, candidate giống nhau nhưng khác nội dung. Chỉ exact duplicate đủ điều kiện mới loại theo policy T01.
- Bỏ historical cross-file joins và dependency registry. Ngừng build cross_file_fingerprint_match/history trên luồng mới; cập nhật sources/docs/test dependencies để DAG không trỏ node bị disabled.
- Unique index workspace source key và index version/disposition chỉ khi có consumer cần; ANALYZE sau bulk build.
- Cập nhật hoặc loại fingerprint tests phù hợp T01. Nếu hash serialization vẫn dùng, giữ golden vectors; nếu một hash bị bỏ hoàn toàn, loại contract tương ứng có lý do.

**Không làm:** append classification qua 24 tháng, lưu lại hai bản JSON, so hash toàn history, đổi formula nghiệp vụ hoặc mất rejected rows.

**Nghiệm thu:** fixture classification đúng expected; physical workspace chỉ chứa scope hiện tại; downstream không tính lại classification qua view; không có registry/history dependency runtime mới.

## T04 — Dimensions dựng một lần

**Files chính:** `models/marts/dimensions/*`, dimension tests và runner initialization.

**Cần làm:**

- Tạo Date/Time theo range cấu hình đủ cho dữ liệu thực và các out-of-file dates hợp lệ; range lấy từ profiling/metadata nhẹ, không quét accepted mỗi batch.
- Giữ technical Unknown/Not Applicable members và key mapping ổn định.
- Zone/geometry/vendor/payment/rate chỉ cập nhật khi reference data thay đổi. Không seed/rebuild tất cả ở mỗi file nếu checksum/reference version không đổi.
- Nếu gặp ngày hợp lệ ngoài range: mở rộng có kiểm soát hoặc fail rõ trước Fact; không âm thầm gán 0 để che thiếu calendar.
- Kiểm unique/index lookup khóa và geometry được lưu một lần ở zone/reference.

**Không làm:** tạo geometry mỗi trip, bỏ dropoff role, đổi key mapping đã công bố.

**Nghiệm thu:** hai batch liên tiếp không rebuild dimension không đổi; keys/unknown/date boundary tests PASS.

## T05 — Fact và quarantine nạp theo file

**Files chính:** `macros/fact_identity_materialization.sql` hoặc materialization mới, `models/marts/facts/*`, `models/quality/*`, control integration.

**Cần làm:**

- Implement đúng lựa chọn T01; bỏ mọi runtime archive event/prior_fact/migration history probe và advisory lock gắn lifecycle không còn cần, nhưng giữ writer serialization phù hợp.
- Giữ PK trip_key/unique source key, file_version_id và các columns đã chốt. Unique_key dbt không thay thế physical constraint.
- Retry xử lý đúng phạm vi file/version, có transaction cho từng thay đổi persistent quan trọng. Nếu chọn delete/insert, không xóa committed version trước khi replacement mới đủ điều kiện.
- Quarantine dùng schema gọn và cùng identity/version scope, không append duplicate ở retry. Empty output phải xóa được reject cũ thuộc scope đang thay thế.
- Reconciliation đọc số lượng/amount từ staging hoặc summary đã chụp trước cleanup. Không phụ thuộc raw JSON đã bỏ khỏi classification.
- Chuẩn bị index Fact(file_version_id), DQ source key/version và reconcile version theo workload; đo chi phí, không thêm mọi fingerprint index.
- Contract mới không cần preserve trip_key chỉ khi T01 xác nhận điều đó; nếu không phải giữ key ổn định.

**Không làm:** drop legacy history/registry, full-refresh Fact live, delete theo source_year_month nếu scope thật là file/version, hoặc thay đổi file khác khi retry.

**Nghiệm thu:** first load/retry/reject-all đúng counts/amounts; file khác giữ nguyên; lỗi trong transaction rollback đúng; key behavior đúng contract; không ghi legacy archive.

## T06 — Bộ tests nhỏ, theo batch, vẫn đủ yêu cầu

**Files chính:** model property YAML, `tests/singular/*`, reconciliation model, `scripts/build_with_detailed_tests.py`, selectors nếu cần.

**Cần làm:**

- Lập mapping tests cũ → giữ/gộp/chuyển full audit/loại vì feature bị bỏ. Không đặt mục tiêu số test tùy ý.
- Batch tests kiểm scope/version, not-null, grain/unique, FK, classification partition, duplicate policy, schema mapping và measures; gom flags/null checks khi giúp giảm scan nhưng vẫn báo rule và số lỗi.
- Reconciliation per file: source = canonical + technical; canonical = Fact + business/duplicate/excluded theo đúng các nhánh; mỗi dòng chỉ nằm một nhánh cuối.
- Đối soát fare, tip, total, doanh thu và eligible counts theo T01. Với cast lỗi không có numeric hợp lệ, tách count eligible và sum, không coi amount không đọc được là zero hợp lệ.
- Full audit riêng kiểm 24-file completeness, global unique/FK, tổng counts/amounts và OLAP parity. Không quét lịch sử lifecycle đã bỏ.
- Failure phải exit nonzero và chặn watermark. `--store-failures` chỉ lưu có kiểm soát, tránh tạo bản sao triệu dòng lỗi; aggregate diagnostics trước, sample có giới hạn.

**Không làm:** sample tests để thay reconciliation đầy đủ của batch; bỏ errors bằng severity warn; dùng LIMIT khiến lỗi ngoài sample bị bỏ qua.

**Nghiệm thu:** negative fixtures gây FAIL đúng; valid fixtures PASS; selectors batch/full audit phân biệt rõ; không còn test ref/source gãy.

## T07 — OLAP nhỏ nhưng đủ dashboard, cập nhật đúng grain

**Files chính:** `models/marts/olap/*`, `macros/replacement_olap_scope.sql` hoặc macro mới, docs Power BI.

**Cần làm:**

- Dùng mapping T01 để giữ số bảng tổng hợp tối thiểu; cung cấp đường truy vấn cho ngày/tháng/năm, giờ/ngày tuần, vendor/borough, payment, passenger group/rate/distance/time, route và pickup/dropoff map.
- Giữ Fact hoặc compatibility views cho drill-down; không tuyên bố ba aggregate hiện tại tự bao phủ mọi requirement khi chưa chứng minh.
- Lưu additive sums và eligible counts; tính average từ sum/count. Tip/speed theo định nghĩa đã chốt, không average-of-averages.
- Với daily grain theo pickup_date: xác định ngày bị tác động rồi aggregate toàn Fact của những ngày đó, kể cả row đến từ file khác. Không chỉ aggregate current file rồi overwrite cả ngày.
- Capture affected grains của old và new trước replacement; delete/rebuild đúng grains, kể cả group không còn dòng. Với monthly source grain phải phân biệt tháng file và tháng phân tích thực tế.
- Cập nhật OLAP sau retry không nhân đôi; kiểm count/revenue và numerator/denominator với Fact. Không CUBE mọi tổ hợp dimension.

**Không làm:** đổi ratio of sums sang mean row ratio hoặc ngược lại ngoài T01; xóa aggregate tháng khác; bỏ measure cần cho nâng cao để giảm bảng.

**Nghiệm thu:** hai file chung ngày, trip lệch tháng, disappeared group, retry đều có aggregate đúng; đủ mapping yêu cầu.

## T08 — Runner skip, retry, publication và cleanup

**Files chính:** `deployment/scripts/run_24_month_backfill.py`, Mage integration/control functions cần thiết, safety tests và logging.

**Cần làm:**

- Init reference/dimensions một lần khi cần; không seed và rebuild dimensions vô điều kiện mỗi file.
- SUCCESS cùng checksum/version: skip trước dbt, giữ metadata quyết định skip. Pending cleanup phải được xử lý trước batch mới.
- FAILED/STAGED retry: kiểm staging, batch/version và writer lock; dựng lại workspace từ input hợp lệ. SUCCESS replacement đã commit không được chạy lại downstream.
- Trình tự: transform DDS → batch tests/reconciliation → OLAP → OLAP tests → commit success/watermark → cleanup. Mỗi phase lỗi có trạng thái retry rõ ràng.
- Không giả định toàn dbt build là một transaction. Trước SUCCESS có thể có DDS đã commit; giữ watermark và publication status, Power BI không refresh trong backfill/batch pending. Nêu rõ giới hạn consumer trực tiếp nếu không có atomic publication.
- Cleanup chỉ truncate staging/workspace đúng current single-writer scope sau SUCCESS; không xóa Fact/DQ/control/file metadata. Cleanup thất bại không làm lại load thành công.
- Log wall-clock phase, invocation, version/checksum và nguyên nhân FAIL. Copy manifest/run_results/logs ngay sau từng invocation seed/run/test/OLAP để tránh bị ghi đè.
- Loại flags/runtime expectations archive/candidate history không còn dùng; invalid legacy option phải báo rõ thay vì bị bỏ qua.

**Không làm:** ép SUCCESS, advance watermark khi lỗi, cleanup trước tests, chọn model resume từ workspace chưa xác thực, chỉnh Mage processing đã ổn ngoài integration cần thiết.

**Nghiệm thu:** test first-run/skip/retry/cleanup-retry và fault ở từng phase PASS; skip không gọi dbt; watermark không đổi khi FAIL.

## T09 — Migration QA và end-to-end

**Cần làm:**

- Tạo schema migration tường minh: old/new columns, compatibility views, constraints/indexes, dependencies, estimated disk requirement và rollback. Không dùng model full-refresh làm migration Fact.
- Ưu tiên database QA nhỏ dựng từ schema/fixture; không restore bản dump lớn nếu chưa có ngân sách disk. Với dữ liệu live chỉ chuẩn bị kế hoạch, không tự chạy migration.
- Loại lifecycle/registry khỏi runtime mới đồng bộ với models/tests/control; bảo tồn legacy data. Ghi rõ giữ bảng cũ nghĩa là chưa thu hồi dung lượng cũ.
- Chạy 2024 fixture rồi 2025 fixture; retry same file; replacement tường minh; fail giữa DDS/test/OLAP/commit/cleanup; kiểm idempotency, isolated scope và rollback/roll-forward.
- Kiểm migration khi có old history table: code mới không bị guard archive cũ chặn. Không đổi fake trạng thái để vượt gate.

**Không làm:** xóa history/registry live để đạt size benchmark, chạy payload full-month trong QA nhỏ hoặc tuyên bố atomic cả pipeline.

**Nghiệm thu:** end-to-end evidence đủ counts/amounts/keys/metadata/OLAP và failure recovery; migration/rollback reviewable; live chưa thay đổi nếu chưa được giao.

## T10 — Benchmark thực tế thời gian và dung lượng

**Điều kiện:** T09 PASS; môi trường, dữ liệu, disk budget và phạm vi chạy được người dùng cho phép. Nếu chưa có, chuẩn bị script/runbook và ghi BLOCKED ở phần phép đo, không tự chạy live.

**Cần làm:**

- So sánh baseline phù hợp (ít nhất feat/fix-dbt và bản tối giản) với cùng input/trạng thái DB/resources. Tách initial load, retry và skip; không so cold first load với warm rerun rồi kết luận.
- Bắt đầu fixture/mẫu có giới hạn cho correctness; full-month để đánh giá thật; tháng tiếp theo kiểm tốc độ khi Fact tăng. Sample không thay thế benchmark tháng.
- Ghi tổng dbt và từng phase/model/test, rows, throughput, largest relation/index, work table heap/TOAST và disk peak theo thời gian.
- Đo permanent delta sau cleanup, WAL/temp activity, host free/allocated VHDX nếu có. pg_stat_database.temp_bytes là lượng ghi tích lũy, không phải peak temp. Không cộng DB và VHDX hai lần.
- Dùng pg_stat_activity/waits/blockers và EXPLAIN (ANALYZE, BUFFERS) SELECT có giới hạn trong môi trường benchmark để xác định nút thắt; query ANALYZE thực sự chạy query.
- Có ngưỡng dừng/timeouts/disk headroom theo máy thực, không sao chép ngưỡng cũ trong progress như số đo hôm nay. Theo dõi đủ thường xuyên để can thiệp được.
- Forecast 24 tháng tách permanent growth với transient peak; tính chi phí legacy giữ lại riêng. Không nhân peak staging một tháng với 24 nếu cleanup tuần tự.

**Không làm:** cam kết dưới 8 phút khi chưa đo; lấy tốc độ Mage làm tốc độ dbt; che failure hoặc dung lượng peak khi báo kết quả.

**Nghiệm thu:** report có số đo và điều kiện tái lập, before/after rõ, correctness PASS; hoặc phần còn thiếu và nguyên nhân minh bạch.

## T11 — Tuning theo evidence và chốt phiên bản

**Cần làm:**

- Chỉ tối ưu các node T10 chứng minh tốn thời gian/disk: projection, join plan, index cần thiết, ANALYZE, tránh scan lặp.
- Nếu một workspace quá chậm, so variant hai bảng gọn với một bảng; chỉ chấp nhận thêm bảng khi tổng thời gian/peak disk đo được phù hợp. Không khôi phục hai bảng JSON rộng mặc định.
- So threads 1/2/4 và session memory trên tài nguyên thực. Kiểm plan spill/hash loops; tính memory nhiều operation/worker, không gán work_mem theo tổng RAM một query.
- Sau mỗi thay đổi đáng kể, chạy lại correctness và benchmark phần bị tác động. Ghi trade-off read/write/index/WAL.
- Cập nhật design/contract nếu variant được chọn thay đổi kiến trúc; không đưa lại registry/archive vì thuận tiện cho test cũ.

**Không làm:** tuning hàng loạt không kiểm chứng, tắt fsync/synchronous_commit trên dữ liệu cần giữ, thêm partitioning phức tạp khi chưa có evidence.

**Nghiệm thu:** cấu hình cuối có rationale và evidence; không có regression correctness; xác định rõ bottleneck còn lại và chi phí lưu trữ.

## T12 — Runbook, full audit và bàn giao

**Cần làm:**

- Viết lệnh init/load file/retry/skip/replacement/full audit/cleanup recovery kèm target/profile/scope và expected trạng thái. Không đưa secret vào ví dụ.
- Cập nhật README, DAG, dictionary, requirements mapping và giới hạn duplicate policy. Đánh dấu tài liệu legacy được thay thế; không xóa evidence cũ.
- Full 24 tháng chỉ chạy khi được giao rõ và môi trường đủ headroom. Trước đó task có thể CODE_READY; không đánh dấu toàn dự án hoàn tất.
- Khi đã chạy: xác nhận đủ 24 file SUCCESS, full audit keys/FK/counts/fare/tip/total, OLAP parity và hai lượt end-to-end/skip/retry; dashboard/insight vẫn cần kiểm riêng.
- Lập decommission proposal cho legacy registry/history sau khi bản mới PASS, backup/retention và rollback rõ. Chỉ xóa khi người dùng cho phép; ghi việc xóa row/table không tự làm VHDX nhỏ ngay.
- Bàn giao số đo cuối, forecast, migration state, các legacy tables còn giữ và việc còn lại. Đừng lẫn “code bỏ tính năng” với “disk đã thu hồi”.

**Không làm:** tuyên bố đủ điểm hoặc đáp ứng toàn đề chỉ vì dbt PASS; chạy lại toàn bộ 24 tháng chỉ để chứng minh skip nếu metadata đủ để kiểm có giới hạn.

**Nghiệm thu:** người khác có thể vận hành và giải thích pipeline; evidence đúng dữ liệu/phạm vi; tất cả giới hạn được ghi rõ.

## 7. Checklist trước khi đóng mỗi task

- [ ] Đã đọc contract, dependencies và kiểm working tree.
- [ ] Thay đổi nằm trong task, không mở rộng sang ingestion hoặc live data ngoài scope.
- [ ] Không mất chức năng phân tích hay kiểm soát ETL tối thiểu.
- [ ] Code/model/test/runner/docs nhất quán, không còn dependency gãy.
- [ ] Test phù hợp đã chạy; phần chưa chạy ghi rõ và không gán DONE sai.
- [ ] Không có secrets/artifacts lớn; diff check sạch.
- [ ] Report Txx có evidence, limitations, rollback và handoff.

## 8. Mẫu yêu cầu giao task cho agent

```text
Thực hiện Txx trong docs/etl_design/DBT_MINIMAL_REFACTOR_TASK_PLAN.md.
Đọc mục 1–6, task Txx và report của task phụ thuộc trước khi sửa.
Kiểm tra branch/working tree hiện tại, giữ nguyên thay đổi đã có.
Chỉ triển khai Txx và integration tối thiểu đã nêu; không tự chạy task sau.
Tuân thủ contract tối giản, các mục “Không làm”, và phạm vi database được giao.
Chạy kiểm chứng phù hợp, cập nhật report Txx và bảng trạng thái.
Nếu chưa có điều kiện kiểm chứng, ghi CODE_READY/BLOCKED cùng lý do cụ thể;
không tuyên bố DONE dựa trên parse hoặc fixture khi task đòi benchmark thật.
```

| Cụm                                        | Tasks   | Kết quả cần đạt                                                                               |
| ------------------------------------------ | ------- | --------------------------------------------------------------------------------------------- |
| **1. Chốt thiết kế và dữ liệu kiểm chứng** | T00–T02 | Inventory, contract tối giản, danh sách bỏ/giữ, fixtures và expected results                  |
| **2. Sửa luồng nạp DDS**                   | T03–T05 | Classification gọn, dimensions dựng khi cần, Fact/DQ nạp theo file, bỏ lifecycle khỏi runtime |
| **3. Hoàn thiện kiểm thử và điều phối**    | T06–T08 | Tests/reconciliation theo batch, OLAP đúng phạm vi, runner skip/retry/cleanup                 |
| **4. Kiểm chứng và tối ưu thực tế**        | T09–T11 | Migration QA, end-to-end/recovery, benchmark thời gian/dung lượng và tuning                   |
| **5. Chốt vận hành và bàn giao**           | T12     | Runbook, full audit, kết quả 24 tháng nếu được giao chạy và kế hoạch xử lý legacy             |

## Kết quả Cụm 1 — 2026-11-19

T00–T02 hoàn tất phạm vi inventory/contract/fixtures. Đọc [contract](DBT_MINIMAL_CONTRACT.md), [T00](../../reports/dbt_minimal_refactor/T00.md), [T01](../../reports/dbt_minimal_refactor/T01.md), [T02](../../reports/dbt_minimal_refactor/T02.md) và [fixture adapter](../../tests/fixtures/dbt_minimal/README.md) trước Cụm 2. Docker đang tắt nên chưa có database metadata mới, SQL end-to-end, migration hoặc benchmark. Tại thời điểm bàn giao Cụm 1, T03–T12 vẫn TODO; xem cập nhật Cụm 2 bên dưới.

## Kết quả Cụm 2 — 2026-11-19

T03–T05 DONE: [implementation guide](DBT_MINIMAL_CLUSTER2_IMPLEMENTATION.md), [T03](../../reports/dbt_minimal_refactor/T03.md), [T04](../../reports/dbt_minimal_refactor/T04.md), [T05](../../reports/dbt_minimal_refactor/T05.md). DDS tối giản bật bằng dbt_minimal_mode, mặc định false. Actual dbt/SQL QA PostgreSQL 18: 13 successful invocations, 12 expected negative failures và 6 selected legacy tests PASS. Không migrate live, không sửa runner, không benchmark tháng. T06–T12 vẫn TODO; Cụm 3 phải hoàn thiện tests/reconciliation, OLAP affected grains và runner trước publication.
