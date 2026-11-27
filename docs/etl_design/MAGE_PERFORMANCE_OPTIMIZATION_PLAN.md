# Kế hoạch tối ưu Mage và bảo toàn yêu cầu đồ án

Ngày rà soát ban đầu: 2026-11-26. Cập nhật hiện trạng: **2026-11-27**.

> Kiến trúc đang chạy đã chuyển sang `direct + streaming + slim-v2`. Database live đã áp dụng migration 008, 009 và 010; biến UI vẫn là `stage_storage_mode: slim`. Một dòng nguồn chỉ được COPY một lần vào `stage.yellow_taxi_trip_data`; Raw và Canonical là hai compatibility view logic, technical reject là phần bù được ghi vào quarantine. Các phần baseline/M00–M05 bên dưới được giữ để giải thích quá trình tối ưu, không phải mô tả physical schema hiện tại.

Trạng thái hiện tại: QA full-month `load_stage` đạt 107,110 giây với 2024-05 (3.723.833 dòng) và 125,800 giây với 2025-05 (4.591.845 dòng). Người dùng đã kiểm chứng 2024-05 trên Mage UI khoảng 2 phút và chạy thành công 2025-05. M06 về lịch end-to-end Mage → dbt → reconciliation → watermark → cleanup vẫn chưa hoàn tất; không suy pipeline Source → DDS đã hoàn thiện chỉ từ kết quả Source → STAGED.

Mục tiêu: Mage Source → Staging **2–4 phút cho một tháng thật**, ưu tiên giữ đúng dữ liệu, incremental, retry, metadata và khả năng tích hợp dbt. Không coi ngưỡng thời gian là lý do bỏ chức năng. Nếu chưa đạt sau tối ưu thì ghi kết quả thực, không tuyên bố đạt hoặc giảm yêu cầu.

## 1. Căn cứ và phạm vi

- Đã đọc bản tổng hợp yêu cầu `D:/BI/DATH/Yeu_cau_do_an_va_noi_dung_3_buoi_seminar.md`, toàn bộ Python, tests và pipeline YAML hiện có trong `mage_project`, cấu hình triển khai liên quan, SQL staging/control và các consumer dbt/runner cần thiết.
- File yêu cầu là **bản tổng hợp đề bài, seminar và thiết kế nhóm**, không phải mọi bảng/cột hoặc ví dụ trong đó đều là ràng buộc bắt buộc. Chưa xác minh lại video/PDF gốc trong lượt rà soát này. Đối chiếu dưới đây dựa trên bản tổng hợp được người dùng chỉ định; không cam kết thay giảng viên về kết quả chấm.
- Không đọc/đưa credentials từ `io_config.yaml` hoặc `.env` vào tài liệu; chỉ xem cấu trúc kết nối qua code và file example.
- Dòng kế hoạch ban đầu không cho phép tác động live vẫn được giữ: các cụm đã sửa code/QA theo yêu cầu tiếp theo, nhưng không migrate/reset/cleanup database live và không chạy lại tháng đã STAGED.
- Kế hoạch dbt/OLAP vẫn theo [T00–T12](DBT_MINIMAL_REFACTOR_TASK_PLAN.md). Mage không nhận nhiệm vụ dựng Fact, OLAP hoặc cập nhật watermark ngay sau STAGED.

## 2. Baseline thực tế

Batch 1, file `yellow_tripdata_2024-05.parquet`: 3.723.833 dòng Raw, 3.723.833 dòng Canonical, 0 technical reject, trạng thái STAGED. Kiểm tra trực tiếp trước đó đã COUNT thật; metadata hiện tại vẫn khớp số lượng. Chưa nạp DDS theo batch counters.

| Phép đo | Kết quả | Nguồn và giới hạn |
| --- | --- | --- |
| Mage từ bắt đầu đến verify_stage | 11 phút 42,478 giây | `heartbeat_at - started_at` batch; ảnh làm tròn khoảng 11 phút 43 giây |
| load_stage | Khoảng 9 phút | Mốc trong ảnh 13:06:06 → 13:15:06, chưa có profiling nội bộ |
| load_source_files / canonicalize_schema | 22 / 44 giây | Ảnh runtime; không đại diện cho mọi tháng |
| Phần còn lại ngoài load_stage | Khoảng 2 phút 43 giây | Bao gồm xử lý và khoảng chờ, không đồng nghĩa tất cả là scheduler |
| Raw gồm indexes | 2.354 MiB | PostgreSQL relation size, làm tròn |
| Canonical gồm indexes | 2.154 MiB | PostgreSQL relation size, làm tròn |
| Toàn staging / database | Khoảng 4,40 / 4,42 GiB | Không gồm nguồn, Mage work, WAL và VHDX |

**Hai bảng trip staging hiện đã là UNLOGGED**: `pg_class.relpersistence = 'u'`, phù hợp DDL `sql/stage/001_stage_tables.sql`. Vì vậy không đề xuất đổi sang UNLOGGED như một tối ưu mới, và không quy toàn bộ 9 phút cho WAL. UNLOGGED có thể bị truncate sau crash; phải có cơ chế phát hiện và dựng lại, không tin trạng thái STAGED còn lưu trong control.

Mỗi bảng trip có ba index: identity PK, unique source_record_key và file SHA. Hai bảng có tổng index khoảng 1,07 GiB theo lần kiểm tra trước. Chưa đo CPU/I/O/constraint time riêng nên các nguyên nhân dưới đây là ứng viên cần profile, không phải tỷ lệ đóng góp đã chứng minh.

## 3. Những gì bắt buộc phải giữ

| Yêu cầu và mục trong bản tổng hợp | Cách giữ khi tối ưu | Bằng chứng nghiệm thu |
| --- | --- | --- |
| Ba nguồn, tối thiểu hai năm; mục 3 | Giữ 24 file 2024–2025, zone CSV, shapefile. Reference nạp một lần/khi đổi checksum, không spatial join từng trip đã có LocationID | Catalog 24 file; lookup 265, geometry 263 theo dữ liệu hiện tại; full audit sau backfill |
| Schema evolution; mục 3.3, 7.4 | Giữ canonical 2024/2025, fee mới, missing cấu trúc khác invalid; giữ nguồn gốc khi cast lỗi | Fixture hai schema, thiếu/đổi type/cast lỗi, mapping và quarantine |
| Nguồn → Staging → DDS; mục 7.2, 8, 12 | Tiếp tục có staging PostgreSQL, dbt nhận đúng contract. Raw vẫn giữ ở phương án đầu tiên | E2E Mage + dbt đúng batch/version, đủ grain/keys/measures |
| LSET/CET; mục 11.2, 12.7, 17 | CET cố định đầu batch, incremental theo source update metadata; chỉ commit sau DDS + tests + reconciliation | Test trong/ngoài cửa sổ, file mới, skip, lỗi downstream không đổi watermark |
| DQ/quarantine; mục 4, 7.4 | Mage xử lý kỹ thuật; dbt xử lý nghiệp vụ/duplicate theo policy. Không loại bản ghi âm/khác thường tùy tiện | Reject giữ giá trị nguồn/lý do; candidate không tự động xóa; DQ tests downstream |
| Reconciliation; mục 7.4, 20 | Footer/source-read = Raw = Canonical + Technical, phân vùng theo identity; DDS đối soát tiền/count riêng | Negative tests thiếu dòng, trùng key, mất reject phải FAIL |
| Metadata/log/retry; mục 7.3, 17 | Giữ batch, file/version, checksum, schema, counters, timestamps, trạng thái, lỗi và lineage | Fault/retry có log, không tạo trùng, không báo STAGED khi thiếu dữ liệu |
| Trigger/schedule; mục 8, 12.8, 20 | Schedule orchestration end-to-end, không chỉ ingestion; không hai writer | Evidence chạy tự động + retry/skip; không dựa vào YAML status |
| Phân tích cơ bản/nâng cao; mục 5–6 | Không bỏ datetime, vendor/payment/rate, passenger/distance, PU/DO, components tiền, geometry hay lineage | Mapping input Mage → DDS → OLAP/BI và measures parity |

Các tên metadata table, mọi trường lặp trên từng row, giữ hai bản dữ liệu rộng vĩnh viễn, CUBE mọi tổ hợp, và thứ tự từng block minh họa không mặc nhiên là yêu cầu bắt buộc. Tuy nhiên **không bỏ chúng khi consumer hiện tại còn phụ thuộc**. Thay đổi kiến trúc phải có migration, mapping và nghiệm thu tích hợp; không dựa vào nhận định “đề không bắt buộc” để phá code đang chạy.

## 4. Các phát hiện trong code

| Vị trí | Phát hiện | Hướng xử lý |
| --- | --- | --- |
| `utils/stage_loader.py`, `data_exporters/load_stage.py` | Mỗi chunk và mỗi bảng tạo engine, introspect schema, chuẩn hóa/copy DataFrame, serialize CSV, COPY bảng tạm rồi INSERT ON CONFLICT vào bảng đích. Đích còn kiểm index/FK/CHECK | Profile từng đoạn; reuse connection/schema; thử direct COPY trong scope được cô lập |
| `utils/database.py` | Mỗi execute/fetch mở/đóng connection mới | Giảm roundtrip/reuse có giới hạn ở từng phase, không transaction khổng lồ |
| `load_source_files.py`, `canonicalize_schema.py`, exporters | Đọc nguồn → ghi raw Parquet → đọc canonicalize → ghi canonical/reject Parquet → đọc lại để nạp DB. Data bind-mount từ Windows | Giảm các vòng I/O; benchmark workspace nội bộ Linux có retention/recovery rõ |
| `canonicalize_schema.py` | Nếu canonical_batch_size nhỏ hơn source chunk, nhiều output trỏ cùng raw_path và mang row_count của subpart. Loader đọc lại cả raw chunk mỗi subpart; unique conflict có thể che việc ghi lặp | Bắt buộc sửa manifest chunk trước khi tune hai batch size độc lập |
| `verify_stage.py` | Chỉ so ba count theo SHA; chưa so footer/local_metrics và chưa chứng minh key coverage từng file. 0 = 0 + 0 hoặc ba tập cùng thiếu có thể PASS | So counts kỳ vọng và partition theo file/version/key, không chỉ SHA tổng |
| `stage_loader.py` | Return len(frame) là số thử ghi; ON CONFLICT có thể ghi ít hơn. Giao cột bằng intersection có thể âm thầm bỏ cột cần thiết | Phân biệt attempted/inserted/conflicted; validate contract trước COPY |
| `canonical.py` | Integer hiện chỉ to_numeric: fractional, range, inf có thể lọt rồi fail tại exporter. Accepted có cast_error_flags nhưng raw_payload=None, nên giá trị lỗi tùy chọn phải truy vết lại từ Raw/source | Chuẩn hóa kỹ thuật đủ type/range; bảo toàn giá trị nguồn lỗi; không ép làm tròn hoặc impute |
| Raw DDL typed | Raw timestamp/bigint/double không nhận mọi giá trị nguồn sai kiểu; technical reject hợp lệ có thể vẫn làm COPY Raw fail | Chốt cách chứa giá trị sai kiểu trước fast path; test contract và các reader dbt; không giả định Raw luôn ghi được |
| `canonicalize_schema.py` cleanup | Xóa output subdir bằng shutil.rmtree nhưng chưa có guard tương tự source loader; dễ phá output khi chạy lại hoặc scope sai | Guard resolved path, writer lock, không xóa phần đang dùng |
| Pipeline YAML | 10 block nối tiếp, run_pipeline_in_one_process=false; triggers=null trong file, scope mặc định chỉ 2024-05 từ selected catalog | Đo queue/process gaps; thử một-process có kiểm chứng. Kiểm lịch thực tế runtime, không suy không có schedule chỉ từ YAML |
| Reference/smoke/tests | Reference validation khá đầy đủ; smoke kết thúc FAILED_AS_EXPECTED có chủ đích. Tests hiện chủ yếu utilities/input, chưa chứng minh full DB retry/crash | Giữ reference và smoke tách khỏi hot path; bổ sung integration test meaningful |

### Hai lỗi cần tránh khi kết luận về hiệu năng

1. Code đã dùng COPY, vì vậy “đổi INSERT sang COPY” chưa đủ mô tả việc phải sửa. Chi phí cần cắt là đường COPY tạm → INSERT đích cùng các vòng chuyển đổi/kiểm tra lặp.
2. Tổng ngoài load_stage gần 2 phút 43 giây. Muốn tổng ≤4 phút với overhead không đổi thì load_stage phải ≤77 giây; đây là yêu cầu rất cao. Cần tối ưu cả overhead và I/O, không chỉ một query.

## 5. Thiết kế được ưu tiên

Giai đoạn M00–M05 ban đầu giữ hai bảng trip Raw/Canonical để giảm rủi ro. Thiết kế được chốt sau đó là `slim-v2`: dùng **một writer và một file/version** tại một thời điểm, một physical trip table và hai compatibility view không làm gãy dbt. Block boundary truyền manifest/path/counters, không truyền DataFrame cả tháng.

Luồng đích:

```text
Batch + CET/LSET → discovery + checksum/schema → filter process/skip
→ đọc nguồn theo bounded chunks → chuẩn hóa/cast + giữ original lỗi
→ một COPY liên tục vào unified trip table; derive technical reject server-side
→ kiểm counts/key partition/source totals → STAGED
→ dbt DDS + tests + reconciliation → commit watermark
→ cleanup staging/work đủ điều kiện
→ OLAP/reporting riêng
```

Fast path direct COPY chỉ dùng khi xác định scope đích sạch hoặc đã reset an toàn cho **attempt hiện tại chưa commit DDS**, không có writer/consumer khác và contract cột đã kiểm. Unique/FK/CHECK vẫn giữ ở bản đầu. COPY không có ON CONFLICT; phải thiết kế retry trước khi bật.

Trên retry dở dang, chọn cơ chế rõ ràng: transaction rollback cả file nếu chi phí chấp nhận được; hoặc checkpoint chunk bền vững với chunk identity, counts và boundary; hoặc rebuild attempt riêng có khóa và publish sau kiểm chứng. Không dùng “xóa mọi staging rồi COPY lại” chung cho các tình huống, nhất là dữ liệu STAGED đang chờ dbt. Slow path hiện tại có thể giữ làm fallback tới khi QA fast path PASS.

## 6. Các task và nghiệm thu

| Task | Trạng thái hiện tại |
| --- | --- |
| M00 | DONE phạm vi baseline/instrumentation và profile loader QA; chưa phải benchmark Mage final |
| M01 | DONE implementation/QA; nullable Raw payload migration chưa áp dụng live |
| M02 | DONE implementation/QA, session cache + Arrow, sample chunk benchmark |
| M03 | DONE opt-in atomic Direct COPY và fault/retry QA; không tự bật live |
| M04–M05 | DONE implementation/QA; candidate opt-in 3:27,860 cho một lượt tháng thật, chưa rollout live |
| M06 | TODO; schedule/recovery end-to-end Mage → DDS → watermark → cleanup chưa nghiệm thu |
| M07 | PARTIAL; full-month QA và hai lượt UI đã chạy, nhưng chưa đủ ba lượt lặp cùng điều kiện và chưa đo Source → DDS |

### M00 — Đóng băng baseline và đo chi tiết

- Chụp diff/hash của các sửa đổi Mage chưa commit; ghi branch/base, runtime package versions, config, machine/Docker CPU/RAM/disk, source SHA và số rows. Không reset mất sửa đổi hiện có.
- Thêm timing quanh read Parquet, cast, JSON/CSV serialization, connect/schema inspection, COPY, INSERT, commit, validation; ghi wall-clock block và inter-block gaps. Ghi source/work/DB/index sizes, RSS peak, temp/WAL delta.
- Benchmark trên database/schema QA được chỉ định; không nạp lại live chỉ để đo. So initial load với initial load cùng điều kiện; retry/skip đo riêng.
- **Nghiệm thu:** phân rã được 9 phút loader và khoảng chờ, đủ căn cứ chọn M02/M03. Không gán toàn bộ thời gian cho engine, index hoặc Docker khi chưa đo.

### M01 — Chốt correctness và contract trước fast path

- Files: canonical utilities, source/canonical manifests, verify_stage, exporters, fixture/test tương ứng.
- Sửa quan hệ source-part/canonical-subpart: Raw chỉ nạp mỗi source-part một lần; offsets/record keys ổn định khi đổi chunk size. Tổng counts tính trên tập source-part riêng, không cộng manifest nhân bản.
- So expected footer với actual source read; Raw DB count đúng kỳ vọng; Canonical/Reject counts đúng local metrics và partition đủ keys, không giao nhau. Kiểm từng file/version, source_row_number đủ coverage theo policy và identity scope đúng. Không chỉ kiểm ba tổng bằng nhau.
- Chốt cách chứa lỗi integer/range/timestamp/numeric/inf; lưu original lỗi cả required và optional. Nếu Raw typed không nhận lỗi, thiết kế cột lỗi payload hoặc landing transport phù hợp, đồng bộ dbt readers; không thay giá trị nguồn âm thầm để vượt DDL.
- Phân biệt attempted/inserted/conflicted; phát hiện conflict lệch payload/version, không coi mọi DO NOTHING là idempotent hợp lệ.
- **Nghiệm thu:** fixture 2024/2025, partial loss, duplicate keys, reject-all, optional bad cast, fractional integer, overflow, chunk sizes khác nhau, quote/newline/backslash/literal `\N`, NULL và empty string cho kết quả đúng. Tập nguồn rỗng hợp lệ khác nguồn mất/không đọc được; policy empty-file hiện inspect_parquet từ chối phải ghi rõ, không đổi âm thầm.

### M02 — Giảm overhead loader với thay đổi nhỏ

- Files: stage_loader, database helpers và exporters.
- Reuse engine/connection cho một load invocation; introspect/validate schema một lần mỗi bảng; dispose/close khi kết thúc. Không chia connection qua process/thread tùy tiện.
- Chuẩn hóa type một lần, tái sử dụng schema/type map; tránh nhiều deep copy hoặc serialize JSON đã là string. Không bỏ kiểm type cần thiết.
- Chọn chunk size bằng phép đo 50k/100k/250k; tăng chỉ khi RAM và thời gian cải thiện. Không load toàn tháng vào DataFrame.
- Đánh giá integer/key/CSV formatting cost; giữ tiền, timestamp, NULL và hash serialization theo contract. Không đổi source_record_key để giảm vài giây.
- **Nghiệm thu:** outputs/key/count/amount parity và fault/retry PASS; report before/after từng phase. Nếu đã đạt 2–4 phút thì không tự mở rộng M03–M05.

### M03 — Direct COPY thay đường nạp hai lượt

- Phụ thuộc M01/M02; chủ trì Mage, phối hợp chủ dbt/runner cho writer lock và trạng thái STAGED.
- Tạo fast path COPY trực tiếp bảng đích, allowlist cột đầy đủ, giữ constraints. Raw/Canonical/Reject phải là cùng attempt và được publish STAGED chỉ sau đầy đủ validation; nếu ghi nhiều transaction, reader dbt không được tiêu thụ attempt pending.
- Chốt retry/crash recovery theo mục 5; không append lại toàn file vào partial target. Không xóa bản đã SUCCESS hoặc dữ liệu file khác. Batch/chunk heartbeat và lỗi phải được ghi qua connection phù hợp, không mất cùng rollback data transaction.
- Giữ fallback legacy loader nhưng không tự fallback từ lỗi correctness để che sai; log rõ mode đã dùng. Skip accepted SUCCESS/version không gọi loader.
- **Nghiệm thu:** fault sau Raw, giữa Canonical, sau reject, trước verify; retry không mất/nhân đôi dữ liệu; consumer không đọc partial; count/key/amount parity. Đo lợi ích thực của direct COPY, không giả định tăng tốc cố định.

### M04 — Giảm intermediate I/O và khoảng chờ Mage — DONE implementation/QA

- Chỉ làm khi M00/M03 còn cho thấy I/O hoặc scheduler là bottleneck.
- Trước tiên thử giữ DAG, đặt workspace tái tạo được trên Docker named volume/Linux filesystem thay bind mount Windows; nguồn vẫn giữ immutable. Thêm cấu hình đường dẫn dùng chung cho Mage và runner, giới hạn disk, retention và cách đọc log/artifact.
- Thử run_pipeline_in_one_process=true trên phiên bản Mage đang cài nếu phù hợp; đo thực tế queue/process gaps và khả năng restart/debug. Không tăng executor_count để tạo nhiều writer.
- Nếu vẫn cần: gộp đọc/canonicalize/load vào hàm streaming bounded chunks dùng chung, các block còn lại điều phối/verify. Không truyền generator hoặc connection qua block serialization. Giữ metrics theo phase, lineage và lỗi; tránh mất khả năng giải thích ETL vì gộp code.
- Hạn chế intermediate raw/empty reject files khi không cần, nhưng chỉ xóa sau trạng thái đủ điều kiện; retry dựng lại từ nguồn có checksum. Giữ reject evidence bền vững trước cleanup.
- **Nghiệm thu:** bounded RSS, workspace có guard path, retry/restart PASS, artifacts đủ, thời gian từ batch start tới STAGED thực sự giảm. Chỉ bật biến thể có lợi; không rewrite toàn bộ sang Spark/Polars chỉ vì kỳ vọng.

### M05 — Dung lượng và index theo evidence — DONE implementation/QA

- Bản đầu giữ cả Raw/Canonical và source key unique. Rà consumer raw_row_id/stage_row_id, joins theo SHA/version và cleanup trước khi đề xuất đổi index/schema.
- Đo từng index cần giữ trên workload Mage + dbt; so tổng Source → DDS, không chỉ COPY. Chỉ bỏ index dư khi query plans và QA downstream chứng minh an toàn; tính cả rebuild index nếu thử build sau load.
- Nếu cần rút gọn metadata lặp trên row, dùng file_version_id liên kết metadata bền vững và compatibility views có tên/type đúng. Đây là thay đổi contract phối hợp người dbt, không thuộc tối ưu loader đơn giản.
- **Không mặc định bỏ Raw vật lý.** Hiện reconciliation/fingerprint/scope/replacement và tests dbt đọc Raw. Biến thể một landing table hoặc Raw view chỉ được nghiên cứu sau khi mapping đầy đủ original values, source identity, technical rejects và dbt parity; không chọn làm lối tắt deadline.
- Cleanup tuần tự sau DDS commit + tests/reconcile PASS; không cleanup ngay sau STAGED. Giữ source/control/quarantine/evidence cần điều tra; xóa bảng không đồng nghĩa VHDX tự thu nhỏ.
- **Nghiệm thu:** có before/after heap/index/work/DB peak, forecast 24 tháng, không mất truy vết hoặc làm dbt chậm hơn tổng thể.

### M06 — Kiểm chứng tích hợp, schedule và phục hồi

- Phối hợp T06/T08/T09 của kế hoạch dbt; Mage DAG hiện kết thúc verify_stage, chưa tự chứng minh end-to-end đã tự động. Runner còn dùng cấu hình riêng và source/canonical_batch_size=50000, phải đồng bộ config, tránh chỉ UI nhanh còn backfill cũ vẫn chậm.
- Giữ CET cố định, kiểm discovery/changed checksum/approval/late file theo policy hiện hành; ghi rõ ngoại lệ file chưa từng nạp so với extraction window. Không đổi source_updated_at thành pickup_datetime để chọn dữ liệu cho tiện.
- Trigger/schedule gọi đúng luồng DDS, lịch reporting riêng. STAGED không advance watermark; DDS/test/reconcile FAIL không advance; cleanup fail sau commit chỉ retry cleanup. Không giả OLAP SUCCESS để vượt gate.
- Vì staging đã UNLOGGED, mô phỏng mất staging sau crash trên QA: control có STAGED nhưng counts/keys thiếu thì phải invalidate/rebuild từ nguồn, không skip hoặc báo thành công. Kiểm cả technical quarantine bền vững còn sót, không append duplicate khi dựng lại.
- **Nghiệm thu:** chạy mới, no-op, retry, source replacement được phép, blocked replacement, lỗi DQ, lỗi dbt và lỗi cleanup đúng trạng thái; evidence tự động ít nhất hai lượt với kịch bản phù hợp. Không tự chạy backfill 24 tháng hoặc crash live.

### M07 — Benchmark thật và bàn giao

- Tháng chuẩn: 2024-05 đủ 3.723.833 dòng. Thêm tháng 2025 có CBD fee để kiểm schema; có phép đo tháng kế tiếp khi DDS tăng. Không dùng sample để công bố đạt 2–4 phút.
- Đo ít nhất hai lượt initial load trong QA được chuẩn bị tương đương; báo từng kết quả và cache conditions, không chỉ lượt nhanh nhất. Retry/skip và download/init reference có số riêng.
- Tổng Mage: từ bắt đầu batch đến verify_stage hoàn tất, tính discovery/checksum/chuẩn hóa/quarantine/load/validation và scheduler gaps. File local đã sẵn có là benchmark chính giống baseline; nếu cần network acquisition, báo tổng bao gồm download riêng.
- Gate Mage: 2–4 phút và correctness PASS. Gate tổng: Mage + dbt DDS + tests/reconcile + điều phối/commit/cleanup ≤10 phút/tháng trên cùng máy, cùng input; không cộng hai số đo riêng ở hai máy.
- Nếu chưa đạt, quay lại phase chiếm thời gian được đo; chỉ chốt khi đạt hoặc báo rõ chưa đạt và bottleneck thực. Không tự hạ chuẩn hoặc ước lượng thời gian thành kết quả.
- Bàn giao commit/config chốt, diff/migration reviewable, lệnh QA/live rõ target, benchmark logs, disk/RSS, mapping yêu cầu, kết quả negative tests, hướng dẫn retry/rollback. Không chứa secret hoặc data trip lớn.

## 7. Các cụm triển khai nên giao lần lượt

| Cụm | Tasks | Quyết định cuối cụm |
| --- | --- | --- |
| 1. Đo và khóa tính đúng | M00–M01 | Có baseline nội bộ và contract/fixtures đủ để đổi loader |
| 2. Tối ưu đường nạp | M02–M03 | Loader gọn, direct COPY có retry đúng; đo xem đã đạt chưa |
| 3. I/O và dung lượng nếu cần | M04–M05 | DONE QA: chọn direct + streaming + slim + deferred index; chưa rollout live |
| 4. Tích hợp và bàn giao | M06–M07 | Schedule/recovery đúng, benchmark tháng thật và tổng hệ thống đạt hoặc ghi rõ chưa đạt |

Không bắt buộc làm mọi biến thể. Nếu M02 đã đạt thì bỏ direct COPY/gộp block/thay schema không cần thiết, nhưng vẫn phải hoàn thành M06–M07. Mục tiêu là hệ thống đơn giản chạy đúng và đủ nhanh.

## 8. Những việc tuyệt đối không dùng để đổi lấy tốc độ

- Bỏ checksum/schema registry, lineage, quarantine, duplicate policy, reconciliation hoặc tests bắt buộc; đổi invalid thành zero/NULL hợp lệ mà không giữ nguyên nhân/giá trị nguồn.
- Giảm phạm vi còn hai tháng/sample rồi báo đủ hai năm; bỏ fee 2025 hoặc PU/DO, payment, rate, passenger để giảm độ rộng.
- Commit watermark tại verify_stage; bỏ qua dbt lỗi; coi UI executed là bằng chứng dữ liệu đủ; dùng counters dự kiến thay DB verification.
- Tắt fsync/durability, đổi control/DDS/quarantine thành UNLOGGED, xóa constraints/unique để COPY nhanh mà không có cơ chế tương đương đã QA.
- Chạy nhiều tháng đồng thời vào staging đang single-writer; truncate dữ liệu STAGED chờ dbt hoặc Fact live; drop volume/history/legacy không được giao.
- Bỏ Raw khi dbt còn đọc; chuyển dashboard đọc trực tiếp nguồn; đưa OLAP trở lại monthly ingestion để sửa lỗi trạng thái.
- Sửa working-tree của người khác không liên quan, công bố số benchmark ước đoán hoặc cam kết mọi yêu cầu hoàn tất trước khi có bằng chứng end-to-end.

## 9. Checklist trước khi chốt

- [ ] Mỗi yêu cầu mục 3 có đường thực thi và minh chứng, không chỉ mô tả thiết kế.
- [ ] Full-month đúng footer/count/keys/amounts, technical errors giữ original; schema 2024/2025 PASS.
- [ ] Retry, no-op, partial failure và UNLOGGED-loss recovery đúng; không nạp trùng/skip thiếu.
- [ ] Mage UI và runner dùng cùng implementation/config; schedule end-to-end có evidence.
- [ ] Mage đạt 2–4 phút thật; tổng tích hợp ≤10 phút hoặc báo chưa đạt rõ ràng.
- [ ] Disk peak/retained/source/work/WAL được phân biệt; cleanup không mất dữ liệu cần dùng.
- [ ] Tài liệu/contract/runbook đã đồng bộ; code_ready khác verified; người dbt nhận bàn giao và kiểm E2E.
