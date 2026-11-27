# Kế hoạch task: tối giản dbt cho đồ án BI

Ngày lập: 2026-11-19. Trạng thái: **Cụm 1–2 (T00–T05) DONE theo phạm vi; DDS opt-in đã kiểm QA, chưa chuyển live**.

Cập nhật kế hoạch: **2026-11-24 — tách OLAP khỏi lượt nạp Source → DDS; phân công bốn người tại mục 9.** Đây là thay đổi kế hoạch, chưa phải xác nhận runner/control đã được sửa. T06 đã hoàn thành trong phạm vi code và QA fixture; T07–T12 vẫn cần triển khai và kiểm chứng. Giữ nguyên bằng chứng T00–T05.

“Tách OLAP khỏi dbt” trong yêu cầu này được triển khai theo nghĩa **không build/test OLAP trong lệnh dbt và runner nạp từng tháng**. Có thể tiếp tục dùng dbt để build OLAP bằng lệnh/job reporting riêng sau khi DDS được nghiệm thu. Không xóa OLAP, không bỏ yêu cầu phân tích, không chuyển việc tổng hợp toàn bộ Fact sang từng visual Power BI.

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
  → commit DDS SUCCESS/watermark
  → cleanup staging/workspace

Sau backfill 24 tháng + full audit DDS PASS:
  DDS + dimensions + metadata bền vững
  → job OLAP riêng → OLAP tests/parity
  → đánh dấu reporting READY → refresh Power BI
```

Đối với lịch incremental sau backfill: nạp file mới → DDS được kiểm chứng → commit watermark DDS; sau đó kích hoạt job reporting riêng. Watermark nạp dữ liệu và checkpoint/reporting status là hai trạng thái khác nhau. Nếu OLAP lỗi, giữ bản reporting đã công bố, ghi FAILED và retry reporting từ DDS; không nạp lại Mage hoặc rollback watermark DDS chỉ vì lỗi OLAP. Trong backfill không tự refresh Power BI theo từng tháng.

Điều chỉnh này thay thế riêng các mô tả **OLAP là gate trước commit watermark/cleanup** trong contract và implementation guide cũ. T08 phải cập nhật các tài liệu đó cùng runtime; các contract grain, DQ, measures và lineage còn lại vẫn giữ. Không thực thi runbook cũ với giả định việc tách đã hoàn thành.

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
| T06  | Tests theo batch và reconciliation                | T05       | DONE       |
| T07  | Job OLAP riêng và nghiệm thu reporting           | T12A + DDS được nghiệm thu | TODO |
| T08  | Runner DDS, skip/retry/cleanup, tách gate OLAP     | T06       | CODE_READY |
| T09  | Migration QA và end-to-end fault/retry            | T08       | DONE       |
| T10  | Benchmark thời gian và dung lượng                 | T09       | IN_PROGRESS — r15 May DDS365.546s NOT MET, June bị ngắt trong ingestion; baseline/tổng tích hợp còn thiếu; r17 đang đo lại cho T11 DDS |
| T11  | Tuning theo evidence, chốt phiên bản              | T10       | IN_PROGRESS — initial Fact tối ưu, DDS-only376.829s; May16models/15tests/full audit/strict parity/fault/recovery/upsert PASS; chưa đạt360s, chưa chạy June |
| T12  | T12A bàn giao DDS; T12B chốt reporting            | A: T11; B: T07 | TODO   |

Thứ tự thực hiện ETL: **T06 → T08 → T09 → T10 ↔ T11 → T12A → người phụ trách chạy 24 tháng và full audit DDS → T07 → T12B**. T07 có thể chuẩn bị code/fixture/mapping sớm, nhưng nghiệm thu reporting thật sau DDS. T12A là mốc bàn giao bản ETL đã benchmark, không đòi phải chạy đủ 24 tháng trước khi bàn giao; T12B không chặn bàn giao ETL. Không đổi số hiệu T00–T12 để giữ liên kết evidence cũ.

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
- Full audit DDS riêng kiểm 24-file completeness, global unique/FK, tổng counts/amounts. OLAP parity thuộc T07, không là gate nạp tháng/cleanup DDS. Không quét lịch sử lifecycle đã bỏ.
- Failure phải exit nonzero và chặn watermark. `--store-failures` chỉ lưu có kiểm soát, tránh tạo bản sao triệu dòng lỗi; aggregate diagnostics trước, sample có giới hạn.

**Không làm:** sample tests để thay reconciliation đầy đủ của batch; bỏ errors bằng severity warn; dùng LIMIT khiến lỗi ngoài sample bị bỏ qua.

**Nghiệm thu:** negative fixtures gây FAIL đúng; valid fixtures PASS; selectors batch/full audit phân biệt rõ; không còn test ref/source gãy.

## T07 — OLAP độc lập sau DDS, phục vụ đủ năm trang dashboard

**Chủ trì:** người 4. **Files chính:** `dbt_project/models/marts/olap/*`, macros/selector reporting, job/script reporting riêng và docs Power BI. Được chuẩn bị sớm bằng fixtures; không là dependency của T08–T12A.

**Cần làm:**

- Tạo entry point riêng cho build/test OLAP; đọc Fact, dimensions và metadata bền vững. Chạy được sau khi staging/classification đã cleanup, không phụ thuộc current batch hoặc workspace còn sót lại.
- Lần đầu sau backfill: build từ toàn bộ DDS đã nghiệm thu, một lần cho tập 24 tháng; kiểm đủ file theo catalog, không suy đủ chỉ từ MIN/MAX ngày. Ba model OLAP hiện hữu phải rà lại filter và cách lấy source month/file lineage để tương thích Fact tối giản; không chỉ bỏ cờ disabled rồi coi là xong.
- Dùng mapping T01 để giữ số bảng tổng hợp tối thiểu; cung cấp đường truy vấn cho ngày/tháng/năm, giờ/ngày tuần, vendor/borough, payment, passenger group/rate/distance/time, route và pickup/dropoff map.
- Giữ Fact hoặc compatibility views cho drill-down; không tuyên bố ba aggregate hiện tại tự bao phủ mọi requirement khi chưa chứng minh.
- Lưu additive sums và eligible counts; tính average từ sum/count. Tip/speed theo định nghĩa đã chốt, không average-of-averages.
- Có trạng thái/job id/checkpoint reporting riêng, ghi phiên bản DDS hoặc tập file/version đã sử dụng; chặn publish/refresh cho tới khi build và parity PASS. Đối với nhiều bảng OLAP, giữ bộ dữ liệu đã công bố bằng schema/version riêng hoặc cơ chế publish tương đương; không để visual đọc hỗn hợp các bảng mới/cũ khi job lỗi.
- Đối với lịch incremental sau này, chọn và đo một trong hai cách: rebuild toàn bộ aggregate nếu đủ nhẹ; hoặc cập nhật affected grains từ checkpoint bền vững. Không bắt buộc xây incremental OLAP phức tạp trước khi có evidence cần thiết.
- Nếu chọn affected grains: lưu old/new grains trước replacement vào metadata bền vững, kể cả group biến mất; khi job reporting chạy phải aggregate toàn Fact của grain, không chỉ current file. Nếu thiếu old scope thì rebuild toàn bộ aggregate, không đoán từ workspace đã cleanup. Phân biệt tháng file với tháng pickup/dropoff.
- Kiểm count/revenue, numerator/denominator với DDS; retry không nhân đôi. Mô phỏng lỗi OLAP: DDS SUCCESS/watermark không thay đổi, chỉ retry job reporting; không cần Mage. Ghi thời gian và dung lượng reporting riêng.
- Bàn giao mapping bảng/cột/grain/measures và câu truy vấn mẫu cho người 3 trước khi chốt semantic model. Cập nhật sơ đồ, hướng dẫn chạy reporting và báo cáo trạng thái thực tế.

**Không làm:** đưa OLAP trở lại gate monthly DDS; phụ thuộc staging tạm; đổi định nghĩa measure; tạo CUBE mọi tổ hợp; xóa Fact hoặc legacy; tuyên bố full 24-month parity chỉ từ fixture; làm hai trang dashboard bằng định nghĩa measure riêng lệch semantic model chung.

**Nghiệm thu:** chạy độc lập sau cleanup; mapping đủ năm trang/yêu cầu; parity DDS PASS; lỗi/retry/publish độc lập được kiểm chứng. Nếu dùng affected grains, kiểm hai file chung ngày, trip lệch tháng và disappeared group. Code/fixture PASS chỉ là CODE_READY; thiếu full DDS thì chưa được ghi DONE cho nghiệm thu dữ liệu thật.

## T08 — Runner skip, retry, publication và cleanup

**Files chính:** `deployment/scripts/run_24_month_backfill.py`, Mage integration/control functions cần thiết, safety tests và logging.

**Cần làm:**

- Init reference/dimensions một lần khi cần; không seed và rebuild dimensions vô điều kiện mỗi file.
- SUCCESS cùng checksum/version: skip trước dbt, giữ metadata quyết định skip. Pending cleanup phải được xử lý trước batch mới.
- FAILED/STAGED retry: kiểm staging, batch/version và writer lock; dựng lại workspace từ input hợp lệ. SUCCESS replacement đã commit không được chạy lại downstream.
- Trình tự: Mage staging → transform DDS → batch tests/reconciliation → persist kết quả đối soát → commit DDS success/watermark → cleanup. Mỗi phase lỗi có trạng thái retry rõ ràng. Không gọi build/test OLAP trong đường chạy này.
- Định nghĩa selector/model enablement cho DDS và reporting riêng; cả `dbt run` lẫn `dbt test` monthly phải loại OLAP và test phụ thuộc OLAP. Không dùng một `dbt test` toàn project sau khi bỏ build OLAP. Selector DDS vẫn bao gồm đầy đủ staging, classification, Fact/DQ, dimensions cần thiết và kiểm thử/reconciliation bắt buộc.
- Kiểm `deployment/scripts/run_24_month_backfill.py`: bỏ lời gọi monthly `DBT_BUILD_OLAP`; sửa logic parent/backfill completion đang đòi `olap_bi_validation_status = 'SUCCESS'`. DDS completion phải xác nhận toàn bộ file được yêu cầu có DDS SUCCESS + reconciliation PASS; không chỉ đếm run đã kết thúc.
- Kiểm `sql/control/003_control_functions.sql` và bảng/status liên quan: tách `DBT_BUILD_OLAP` khỏi required steps cho DDS commit; giữ DDS tests/reconciliation bắt buộc. Không gán giả OLAP SUCCESS để vượt gate. Thiết kế migration tương thích trạng thái legacy, tách DDS status và reporting status/checkpoint; không advance nhầm source watermark khi chỉ reporting thành công.
- Không giả định toàn dbt build là một transaction. Trước SUCCESS có thể có DDS đã commit; giữ watermark và publication status, Power BI không refresh trong backfill/batch pending. Nêu rõ giới hạn consumer trực tiếp nếu không có atomic publication.
- Cleanup chỉ truncate staging/workspace đúng current single-writer scope sau SUCCESS; không xóa Fact/DQ/control/file metadata. Cleanup thất bại không làm lại load thành công.
- Log wall-clock phase, invocation, version/checksum và nguyên nhân FAIL. Copy manifest/run_results/logs ngay sau từng invocation seed/run/test; reporting có thư mục/job id riêng, không ghi đè artifact DDS.
- Loại flags/runtime expectations archive/candidate history không còn dùng; invalid legacy option phải báo rõ thay vì bị bỏ qua.
- Cập nhật contract/implementation guide/README cũ còn yêu cầu OLAP trước watermark. Ghi rõ Mage DAG hiện không tự chứng minh đã có end-to-end schedule; cấu hình và kiểm trigger/schedule gọi orchestration DDS đúng, reporting lịch riêng hoặc trigger sau DDS, tránh hai writer đồng thời.

**Không làm:** ép SUCCESS, advance watermark khi lỗi, cleanup trước tests, chọn model resume từ workspace chưa xác thực, chỉnh Mage processing đã ổn ngoài integration cần thiết.

**Nghiệm thu:** first-run/skip/retry/cleanup-retry và fault từng phase PASS; skip không gọi dbt; lỗi trước DDS commit không advance watermark. Monthly chạy thành công dù chưa có bảng OLAP; artifacts chứng minh không chạy OLAP model/test. Lỗi reporting sau DDS commit không làm đổi watermark; parent DDS complete không đòi OLAP SUCCESS. Lỗi cleanup sau commit giữ DDS SUCCESS và có recovery riêng.

## T09 — Migration QA và end-to-end

**Cần làm:**

- Tạo schema migration tường minh: old/new columns, compatibility views, constraints/indexes, dependencies, estimated disk requirement và rollback. Không dùng model full-refresh làm migration Fact.
- Ưu tiên database QA nhỏ dựng từ schema/fixture; không restore bản dump lớn nếu chưa có ngân sách disk. Với dữ liệu live chỉ chuẩn bị kế hoạch, không tự chạy migration.
- Loại lifecycle/registry khỏi runtime mới đồng bộ với models/tests/control; bảo tồn legacy data. Ghi rõ giữ bảng cũ nghĩa là chưa thu hồi dung lượng cũ.
- Chạy 2024 fixture rồi 2025 fixture; retry same file; replacement tường minh; fail giữa DDS/test/reconciliation/commit/cleanup; kiểm idempotency, isolated scope và rollback/roll-forward. Chạy QA DDS khi OLAP chưa được dựng; kiểm scheduled incremental bằng file mới rồi chạy lại cùng input để chứng minh skip/idempotency, không chỉ chạy tay backfill.
- Kiểm negative gate: DDS test/reconcile lỗi thì không commit; thiếu OLAP không chặn DDS commit hoặc parent completion; không có đường ép OLAP SUCCESS. Việc build/parity và lỗi job reporting đầy đủ thuộc T07.
- Kiểm migration khi có old history table: code mới không bị guard archive cũ chặn. Không đổi fake trạng thái để vượt gate.

**Không làm:** xóa history/registry live để đạt size benchmark, chạy payload full-month trong QA nhỏ hoặc tuyên bố atomic cả pipeline.

**Nghiệm thu:** end-to-end DDS evidence đủ counts/amounts/keys/metadata, schedule và recovery; độc lập với OLAP; migration/rollback reviewable; live chưa thay đổi nếu chưa được giao.

## T10 — Benchmark thực tế thời gian và dung lượng

**Điều kiện:** T09 PASS; môi trường, dữ liệu, disk budget và phạm vi chạy được người dùng cho phép. Nếu chưa có, chuẩn bị script/runbook và ghi BLOCKED ở phần phép đo, không tự chạy live.

**Cần làm:**

- So sánh baseline phù hợp (ít nhất feat/fix-dbt và bản tối giản) với cùng input/trạng thái DB/resources. Tách initial load, retry và skip; không so cold first load với warm rerun rồi kết luận.
- Bắt đầu fixture/mẫu có giới hạn cho correctness; full-month để đánh giá thật; tháng tiếp theo kiểm tốc độ khi Fact tăng. Sample không thay thế benchmark tháng.
- Mục tiêu nhóm: **Mage 2–4 phút, dbt DDS 3–6 phút, tổng Source → DDS ≤ 10 phút/tháng** trên máy chốt benchmark. Đây là mục tiêu cần đo, không phải số đo hiện có. dbt DDS tính cả build, tests và reconciliation; tổng tính cả điều phối, metadata commit và cleanup. Hai chỉ tiêu thành phần đạt chưa đủ nếu tổng vẫn quá 10 phút.
- Chuẩn hóa ranh giới: benchmark chính dùng file nguồn đã sẵn có; ghi rõ checksum/I/O nguồn, thời gian Mage và dbt, overhead còn lại. Nếu đo cả download thì báo thêm tổng riêng, không giấu download; init reference/dimensions lần đầu ghi riêng và vẫn báo tổng lần đầu. Không trộn cached rerun với first load.
- Ghi tổng dbt và từng phase/model/test, rows, throughput, largest relation/index, work table heap/TOAST và disk peak theo thời gian.
- Đo permanent delta sau cleanup, WAL/temp activity, host free/allocated VHDX nếu có. pg_stat_database.temp_bytes là lượng ghi tích lũy, không phải peak temp. Không cộng DB và VHDX hai lần.
- Dùng pg_stat_activity/waits/blockers và EXPLAIN (ANALYZE, BUFFERS) SELECT có giới hạn trong môi trường benchmark để xác định nút thắt; query ANALYZE thực sự chạy query.
- Có ngưỡng dừng/timeouts/disk headroom theo máy thực, không sao chép ngưỡng cũ trong progress như số đo hôm nay. Theo dõi đủ thường xuyên để can thiệp được.
- Forecast 24 tháng tách permanent growth với transient peak; tính chi phí legacy giữ lại riêng. Không nhân peak staging một tháng với 24 nếu cleanup tuần tự.

**Không làm:** bỏ tests/OLAP requirement để báo nhanh; tính thời gian OLAP vào benchmark monthly DDS sau khi đã tách; che chi phí OLAP khỏi báo cáo toàn hệ thống. Báo riêng thời gian backfill DDS, full audit, OLAP và Power BI refresh. Không lấy tốc độ Mage làm tốc độ dbt hoặc che failure/dung lượng peak.

**Nghiệm thu:** report có số đo tái lập, before/after, correctness PASS và đạt/chưa đạt từng ngưỡng; ít nhất một tháng thật và tháng tiếp theo có lịch sử. Không DONE mục tiêu hiệu năng nếu chưa có full-month evidence. Chuyển T11 nếu chưa đạt; sau tích hợp Mage mới phải đo lại tổng, không cộng hai số đo ở hai máy khác nhau.

## T11 — Tuning theo evidence và chốt phiên bản

**Phạm vi được người dùng chốt ngày27/11/2026:** chỉ tối ưu và nghiệm thu DDS; Mage có người khác phụ trách. T11 DDS cần build+tests+reconciliation ≤360s cho tháng đầu và tháng kế tiếp có lịch sử, cùng full correctness/strict parity và retry/skip. Mục tiêu Mage≤240s và tổng Source→DDS≤600s vẫn giữ trong T10/nghiệm thu tích hợp; không dùng kết quả Mage để chặn hoặc giả PASS riêng DDS. Không sửa code Mage trong task này. Original baseline bị timeout vẫn phải báo tại T10; parity trước/sau T11 dùng pre-T11 minimal đã chạy thành công, ghi đúng nhãn.

**Cần làm:**

- Chỉ tối ưu các node T10 chứng minh tốn thời gian/disk: projection, join plan, index cần thiết, ANALYZE, tránh scan lặp.
- Nếu một workspace quá chậm, so variant hai bảng gọn với một bảng; chỉ chấp nhận thêm bảng khi tổng thời gian/peak disk đo được phù hợp. Không khôi phục hai bảng JSON rộng mặc định.
- So threads 1/2/4 và session memory trên tài nguyên thực. Kiểm plan spill/hash loops; tính memory nhiều operation/worker, không gán work_mem theo tổng RAM một query.
- Sau mỗi thay đổi đáng kể, chạy lại correctness và benchmark phần bị tác động. Ghi trade-off read/write/index/WAL.
- Cập nhật design/contract nếu variant được chọn thay đổi kiến trúc; không đưa lại registry/archive vì thuận tiện cho test cũ.
- Vòng lặp: đo T10 → tìm bottleneck → sửa có giới hạn → kiểm correctness → đo lại cùng điều kiện. Chỉ chốt khi đúng dữ liệu và đạt mục tiêu; nếu vẫn chưa đạt thì ghi nguyên nhân, phương án và chi phí, không tự hạ ngưỡng hoặc báo đạt bằng sample. Không dùng vô hạn thời gian để theo đuổi ngưỡng mà thiếu evidence tiến bộ.

**Không làm:** tuning hàng loạt không kiểm chứng, tắt fsync/synchronous_commit trên dữ liệu cần giữ, thêm partitioning phức tạp khi chưa có evidence.

**Nghiệm thu:** cấu hình cuối có rationale và evidence; không có regression correctness; xác định rõ bottleneck còn lại và chi phí lưu trữ.

## T12 — Hai mốc bàn giao: DDS trước, reporting sau

### T12A — Bàn giao ETL cho người chạy 24 tháng (sau T11)

- Viết lệnh init/load file/retry/skip/replacement/full audit/cleanup recovery kèm target/profile/scope và expected trạng thái. Không đưa secret vào ví dụ.
- Cập nhật README, DAG, dictionary, requirements mapping và giới hạn duplicate policy. Đánh dấu tài liệu legacy được thay thế; không xóa evidence cũ.
- Full 24 tháng chỉ chạy khi được giao rõ và môi trường đủ headroom. T12A được DONE khi đủ điều kiện bàn giao ETL/benchmark cho người chạy; ghi riêng backfill/full audit là PENDING cho tới khi có bằng chứng. Không đánh dấu toàn T12 hoặc toàn dự án hoàn tất trước T12B.
- Trước bàn giao: có commit/config chốt, dependency/env không secret, lệnh và thứ tự chạy, benchmark một tháng/tháng kế tiếp, forecast disk, ngưỡng dừng và recovery đã QA. Không cần đợi T07/Power BI để bàn giao DDS.
- Khi người phụ trách đã chạy: xác nhận đủ 24 file DDS SUCCESS, full audit keys/FK/counts/fare/tip/total và evidence end-to-end/skip/retry. Ghi mốc DDS nghiệm thu riêng; OLAP parity thuộc T07, chưa có OLAP không làm DDS thành FAILED.
- Lập decommission proposal cho legacy registry/history sau khi bản mới PASS, backup/retention và rollback rõ. Chỉ xóa khi người dùng cho phép; ghi việc xóa row/table không tự làm VHDX nhỏ ngay.
- Bàn giao số đo cuối, forecast, migration state, các legacy tables còn giữ và việc còn lại. Đừng lẫn “code bỏ tính năng” với “disk đã thu hồi”.

### T12B — Chốt reporting và tài liệu cuối (sau T07)

- Gắn evidence full audit DDS, OLAP parity/publish và refresh Power BI vào một runbook thống nhất; ghi thời gian/disk mỗi phần riêng. Không biến chúng thành monthly watermark gate.
- Hoàn thiện mapping yêu cầu → bảng/measure/trang dashboard → test/ảnh/minh chứng; kiểm đủ năm trang, bộ lọc, drill-down/roll-up/slice/dice theo yêu cầu thực tế và phân tích nâng cao đã chọn.
- Mỗi người viết phần mình, có người tích hợp báo cáo/slide/demo; phân biệt kết quả thực đo với mục tiêu hoặc CODE_READY. T12B không DONE khi mới có khung dashboard.

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
| **3. Hoàn thiện ETL DDS độc lập** | T06, T08, T09 | Tests/reconciliation, runner/control tách OLAP, QA/recovery và schedule |
| **4. Đo, tối ưu và bàn giao DDS** | T10, T11, T12A | Benchmark/tuning, tích hợp Mage mới, bàn giao bản chốt cho người chạy 24 tháng |
| **5. Reporting và chốt final** | T07, T12B | Sau DDS được nghiệm thu: OLAP riêng, Power BI và evidence/tài liệu cuối |

## 9. Phân công bốn người và quy tắc phối hợp

Phân công theo yêu cầu ngày 2026-11-24. T07 chuyển cho người 4; không còn nằm trong phần tối ưu monthly của người 2. Mỗi chủ trang chịu trách nhiệm visuals, interaction, đối soát, insight và nội dung báo cáo của trang, sử dụng semantic model chung.

| Người | Trước khi có DDS đầy đủ | Sau khi có DDS đầy đủ | Bàn giao |
| --- | --- | --- | --- |
| **1 — Bạn: Mage và tích hợp** | Tối ưu Mage **2–4 phút**; giữ staging/schema/lineage/DQ contract. Cùng người 2 đo integrated run, nhận bản T12A rồi chạy 24 tháng khi được giao và đủ disk budget | Full audit DDS cùng người 2; làm **trang 4: Không gian, tuyến và Top N**; tích hợp báo cáo/bản nộp từ đóng góp của cả nhóm | Commit/config Mage, phase timings, staging contract, log 24 file, disk và full audit |
| **2 — Bạn phụ trách dbt DDS** | Đọc evidence T00–T05; hoàn thiện **T06/T08/T09/T10/T11/T12A**. Test với Mage hiện có, sau đó đo lại cùng Mage mới. Mục tiêu dbt DDS **3–6 phút**, chưa đạt thì đo/tối ưu lại | Hỗ trợ backfill/full audit; làm **trang 5: Phân tích nâng cao** | Code/runbook tái lập, correctness/retry/schedule, benchmark thật, disk forecast, known limitations. T07 không chặn bàn giao |
| **3 — Bạn phụ trách nền tảng dashboard** | Dựng khung chung đủ năm trang; chốt kết nối, semantic model, measures và cách ghép trang với người 4. Làm sớm bằng fixture/sample có ghi nhãn | Tích hợp năm trang; làm **trang 2: Doanh thu và xu hướng**; kiểm navigation/filter/refresh toàn báo cáo | Template/theme/sidebar/header, model/measure dictionary, quy tắc phát triển trang, Power BI tích hợp |
| **4 — Bạn phụ trách OLAP** | Chuẩn bị **T07**, mapping grain/measures cùng người 3, test reporting riêng bằng fixture; dựng hai trang từ template | Chạy/nghiệm thu OLAP sau full DDS; làm **trang 1: Tổng quan** và **trang 3: Thanh toán, hành khách và loại cước** | Job reporting riêng, parity/publish/retry evidence, hai trang đã đối soát và phần báo cáo OLAP |

### 9.1. Hợp đồng và phạm vi sở hữu

1. **Mage ↔ dbt:** chốt tên/schema/cột/type, batch/file_version_id/checksum, staging-ready và lỗi/retry trước khi tối ưu song song. Người 1 giữ Mage ingestion; người 2 giữ dbt models/tests và runner/control. Thay giao diện phải cập nhật contract và fixture chung trước khi gộp, không tự đổi schema đang dùng.
2. **DDS ↔ OLAP ↔ BI:** người 2 bàn giao DDS dictionary/định nghĩa measure; người 4 giữ bảng/view reporting và grain; người 3 giữ semantic model/DAX chung. Chốt sớm bằng fixtures, không đợi 24 tháng mới làm BI. Người 4 không tự thay DDS grain; chủ trang không tự đổi doanh thu/tip/speed.
3. **Tích hợp Power BI:** người 3 giữ bản chính. Ưu tiên PBIP với phạm vi file trang riêng nếu công cụ hỗ trợ; nếu PBIX thì mỗi người làm bản riêng từ cùng template và người 3 ghép từng đợt. Không đồng thời ghi đè một PBIX chung, không mặc định Git merge được PBIX. Thử ghép một trang sớm để xác nhận cách bàn giao khả thi.
4. **Measure phát sinh:** chủ trang đề xuất công thức/grain/filter và truy vấn đối soát; người 3 tích hợp DAX, người 4 kiểm mapping OLAP. Không tạo bốn bộ measures trùng tên khác logic.
5. **Báo cáo:** mỗi người viết phần mình, cung cấp ảnh/insight/tests/hạn chế; người 1 tích hợp bản cuối. Không dồn toàn bộ báo cáo cho người 4 vốn đã nhận OLAP và hai trang. Người 3 hỗ trợ chuẩn hóa ảnh dashboard.

### 9.2. Phần chung và phạm vi năm trang

Phần chung không chỉ là sidebar/header. Người 3 chuẩn bị theme/layout, navigation, connection mode/refresh, relationships và role-playing dimensions, measures/format tiền/đơn vị, slicers chung, tooltip/drill-through khi cần, cách xử lý blank/zero. Kiểm ambiguous relationships và double counting giữa Fact và aggregate. Chỉ thêm RLS khi yêu cầu/thực tế cần, không tự mở rộng phạm vi.

| Trang / người | Nội dung tối thiểu và kiểm chứng |
| --- | --- |
| **1. Tổng quan / người 4** | KPI chuyến, hành khách, doanh thu; thời gian ngày/tháng/năm, vendor/borough; đối soát KPI với DDS/OLAP cùng filter |
| **2. Doanh thu / người 3** | Xu hướng doanh thu/chuyến theo tháng, giờ/ngày tuần, vendor/borough; drill-down/roll-up, so sánh thời gian; phân biệt tháng file với ngày phân tích |
| **3. Cơ cấu / người 4** | Payment, nhóm hành khách, RateCode; liên hệ distance/duration/amount và eligible counts; không cộng trung bình các nhóm |
| **4. Không gian / người 1** | Pickup/dropoff role, map zone/borough dùng geometry, tuyến và Top N theo thời gian; kiểm LocationID cả hai đầu chuyến |
| **5. Nâng cao / người 2** | Tip/speed, doanh thu/chuyến, MoM/YoY theo định nghĩa chung; so sánh giai đoạn/nhóm theo mapping yêu cầu; giải thích biến động nhiều chiều, có insight và hạn chế |

Đây là phân bổ trang đề xuất, phải đối chiếu mapping T01/yêu cầu trước khi chốt visual. Mỗi trang có câu hỏi phân tích, thao tác OLAP phù hợp, SQL đối soát cùng filter và nhận xét có căn cứ. Năm trang không tự chứng minh đáp ứng toàn bộ đề; yêu cầu chưa được bao phủ phải bổ sung vào trang phù hợp. Không dùng số fixture để kết luận cho 24 tháng. AI chỉ bổ sung khi đã đối chiếu yêu cầu và mục tiêu, không thay thế phân tích nâng cao.

### 9.3. Các cổng nghiệm thu

- **A — Chuẩn bị song song:** người 1 tối ưu Mage, người 2 hoàn thiện DDS, người 3 dựng template/model, người 4 chuẩn bị OLAP. Chốt contract/fixture trước; không chờ ETL xong mới bắt đầu dashboard.
- **B — Chốt ETL:** correctness/reconcile/retry PASS; Mage 2–4 phút, dbt DDS 3–6 phút, **tổng thực đo ≤10 phút/tháng** trên cùng máy/đầu vào, tính cả overhead/commit/cleanup. Kiểm thêm tháng tiếp theo có lịch sử. Chưa đạt thì T10 ↔ T11; không giảm tests hoặc chốt chỉ từ hai số đo riêng.
- **C — Backfill:** người 1 nhận commit/config T12A đóng băng, chạy tuần tự 24 tháng có cleanup/disk monitoring; người 2 hỗ trợ lỗi. Full audit đủ 24 file PASS mới mở reporting thật. Sửa code giữa backfill phải ghi version và đánh giá ảnh hưởng dữ liệu đã nạp.
- **D — Final:** người 4 nghiệm thu T07; người 3 refresh/tích hợp; bốn người hoàn thiện trang và đối soát chéo với DDS. Kiểm incremental/schedule/reporting retry, hoàn thiện báo cáo/slide/demo/gói nộp. DDS SUCCESS chưa phải final hoàn tất.

**Cân đối khối lượng:** người 4 nhận OLAP và hai trang nên dùng hai trang tổng quan/cơ cấu với measures chung; không giao thêm toàn bộ semantic model hay toàn bộ báo cáo. Người 2 vẫn phải hỗ trợ lỗi backfill khi làm trang nâng cao; giữ bản ETL chốt và ghi ticket cụ thể để tránh sửa chéo.

## Kết quả Cụm 1 — 2026-11-19

T00–T02 hoàn tất phạm vi inventory/contract/fixtures. Đọc [contract](DBT_MINIMAL_CONTRACT.md), [T00](../../reports/dbt_minimal_refactor/T00.md), [T01](../../reports/dbt_minimal_refactor/T01.md), [T02](../../reports/dbt_minimal_refactor/T02.md) và [fixture adapter](../../tests/fixtures/dbt_minimal/README.md) trước Cụm 2. Docker đang tắt nên chưa có database metadata mới, SQL end-to-end, migration hoặc benchmark. Tại thời điểm bàn giao Cụm 1, T03–T12 vẫn TODO; xem cập nhật Cụm 2 bên dưới.

## Kết quả Cụm 2 — 2026-11-19

T03–T05 DONE: [implementation guide](DBT_MINIMAL_CLUSTER2_IMPLEMENTATION.md), [T03](../../reports/dbt_minimal_refactor/T03.md), [T04](../../reports/dbt_minimal_refactor/T04.md), [T05](../../reports/dbt_minimal_refactor/T05.md). DDS tối giản bật bằng dbt_minimal_mode, mặc định false. Actual dbt/SQL QA PostgreSQL 18: 13 successful invocations, 12 expected negative failures và 6 selected legacy tests PASS. Không migrate live, không sửa runner, không benchmark tháng. Tại mốc Cụm 2, T06–T12 còn TODO. Cập nhật 2026-11-24: Cụm 3 hoàn thiện tests/reconciliation và runner/control DDS độc lập; OLAP chuyển sang T07 sau nghiệm thu DDS, không chặn watermark hoặc bàn giao ETL.

## Kết quả T06 — 2026-11-24

T06 DONE trong phạm vi code và QA fixture: [report](../../reports/dbt_minimal_refactor/T06.md). Batch selector 15 test kiểm scope, DQ, Fact, khóa và đối soát fare/tip/total; full audit selector độc lập nhận danh sách file từ catalog. QA có positive/negative PASS và đối chiếu literal oracle T02. Chưa chạy dữ liệu thật/24 file hoặc nối runner vào selector; việc chặn watermark bằng exit code test thuộc T08. Không suy kết quả fixture thành benchmark tháng.

## Kết quả T08 — 2026-11-24

T08 **CODE_READY**: [report](../../reports/dbt_minimal_refactor/T08.md). Runner DDS đã dùng selector run/test T06, bỏ OLAP gate và parent OLAP dependency, kiểm skip/retry/cleanup, lưu reconciliation và artifact riêng từng dbt invocation. Safety tests 26 PASS; dbt thực trên fixture QA qua runner PASS 16 model/15 test, negative test exit 1 và reconciliation âm tính báo lỗi đúng. Chưa chạy toàn orchestration trên PostgreSQL/PostGIS QA với control schema/migration và chưa xác nhận schedule/trigger. Không migrate live, không chạy T09, không ghi DONE từ fixture/mock.

## Trạng thái mới nhất T10/T11 và dọn source — 2026-11-27

T10/T11 giữ **IN_PROGRESS**, bản sửa DDS **CODE_READY**. Lượt một tháng May thật mới nhất nạp3,723,833raw, Fact3,661,046/business62,787/duplicate0;16models/15tests/đối soát/commit/cleanup PASS. DDS411.906s chưa đạt360s; Mage595.328s. Chỉ chạy May theo yêu cầu mới, không nạp June. Original baseline chưa có full native parity; không thay bằng sample hoặc recovery. Xem [T10](../../reports/dbt_minimal_refactor/T10.md), [T11](../../reports/dbt_minimal_refactor/T11.md).

Đã dọn bộ công cụ benchmark/QA tạm và bản plan root trùng theo yêu cầu người dùng. Giữ source Source→DDS, migration/rollback, contract và regression tests. Dùng [dbt README](../../dbt_project/README.md) cho entrypoint chính; các lệnh campaign QA cũ không còn được giữ trong source. Snapshot/log cục bộ ở `data/work`, không commit. Sau dọn:45offline tests PASS, dbt parse minimal/legacy PASS;89SQL/YAML/CSV dbt giữ nguyên so bản đã kiểm. Không migrate live, xóa database hoặc thực hiện T12/T07.

## Kết quả T09 — 2026-11-24

T09 **DONE trong phạm vi QA/migration plan**: [report](../../reports/dbt_minimal_refactor/T09.md), [migration guide](DBT_MINIMAL_T09_MIGRATION.md). QA PostgreSQL 16/PostGIS cùng image Compose nạp lookup/shapefile thật; hai file mẫu PASS 16 model/15 test, fault/retry/replacement và full audit ba file latest PASS. Windows Task Scheduler tự chạy hai lượt (nạp mới/skip), exit 0; DDS parent SUCCESS khi OLAP PENDING. Migration metadata 008/rollback/reapply PASS; đã khảo sát schema-only/size/dependencies database hiện có bằng truy vấn chỉ đọc. Safety suite 29 PASS. Database hiện có giữ nguyên; triển khai live vẫn cần xử lý batch dở, backup, budget T10 và xác nhận consumer. Không chạy T10 hoặc 24 file.
