# Tiến độ tối ưu lưu trữ

## Task 4–5 — Recovery publication, parity và hồ sơ đo

### Replacement recovery (PASS ở QA)

- Migration 007 lưu cặp old/new và bản ghi approval theo batch. Runner resume dùng lại chính cặp đó. Runner và trigger PostgreSQL chặn batch master mới khi replacement còn pending. Nếu batch đã commit, resume trả `already_committed` mà không chạy lại dbt hoặc tăng watermark.
- Trên bản clone QA riêng, batch 9 xử lý fixture 131 dòng. Fault trong transaction Fact rollback Fact/archive. Fault sau model OLAP đầu tiên giữ Staging, watermark và published reconciliation. Fault SQL sau `commit_batch_success` nhưng trước COMMIT rollback watermark và reconciliation cùng nhau. Retry batch 9 kết thúc SUCCESS, watermark tăng đúng một lần, không tăng archive/trip keys khi gọi lại. Retire version 4, version 2 giữ 4.449 Fact, version 8 có 129 Fact, ba grain OLAP cùng 4.578 trip. Trigger chặn batch mới trong transaction rollback.
- Sửa test duplicate survivor để full check theo Staging version đang chạy. Với duplicate cũ còn kiểm điều kiện occurrence và survivor key persisted. Các dbt model commit độc lập, nên recovery sau lỗi giữa models là roll-forward. Consumer đọc trực tiếp bảng có thể thấy dữ liệu dở dang trước watermark. Production vẫn cần quyết định cô lập publisher/consumer và triển khai migration 007.
- Evidence: `evidence/run_logs/storage_qa/task4_replacement_recovery.log` (Fact rollback và phát hiện test survivor) và `task4_replacement_recovery_retry.log` (OLAP fault, commit rollback, retry). Suite 37/37, py_compile và git diff --check PASS.

### Historical parity (PARTIAL)

- Kiểm metadata read-only xác nhận restore chỉ có version 5 (2024-05), không có cặp cross-file và thiếu cả ba nhánh persisted. Thư mục backup đã biết chỉ có dump đã restore. Giữ evidence mẫu thật 2.048/2.048. Cần thêm snapshot thật nhiều version, hoặc duyệt rebuild baseline độc lập và candidate từ Parquet bất biến trên môi trường đủ dung lượng.

### Phép đo dung lượng và sign-off (kế hoạch sẵn sàng, FIN-15 BLOCKED)

- D trống 43.266.707.456 byte, dự phòng 31.457.278.976 byte. Ngân sách thêm 11.809.428.480 byte (~11,00 GiB), hoặc 9.661.944.832 byte (~9,00 GiB) sau biên dừng 2 GiB. Đây là snapshot, chưa có peak đo được.
- [Hồ sơ quyết định](deployment/runbooks/STORAGE_PILOT_SIGNOFF.md) bổ sung giới hạn tổng 20.000 dòng, baseline, kiểm session/timeout, ngưỡng dừng có tính độ trễ, phân biệt cap giám sát với quota vật lý và cách báo cáo forecast. Phép đo chưa chạy. Project Lead/Integration và dbt Owner còn PENDING, cần quyết định riêng cho phép thử QA và nguồn parity. Pilot và Task 6 vẫn NO-GO.

## Blocker 1 — Batch 34 resolved

- Xác minh SHA-256 của backup đã restore và Parquet version 5. Heartbeat Batch 34 dừng ở 2026-11-18 15:11:40 UTC, không có worker dbt/phiên ghi active, Raw/Canonical rỗng, không có reconciliation hoặc watermark SUCCESS cho batch này.
- Transaction giới hạn lock/statement, dùng advisory lock của runner và khóa Staging, kiểm lại metadata rồi gọi control functions để đặt riêng step DBT_BUILD_DDS và Batch 34 thành FAILED (`DOWNSTREAM_STAGING_LOST`). Watermark vẫn 1970-01-01 và last successful batch NULL. Version 5 vẫn có thể được restage từ Parquet đã xác minh trong một run được duyệt.

## Blocker 2 — Replacement scope trên Fact QA

- Macro yêu cầu đúng một cặp version cũ/mới theo batch, Parquet đã stage và approval có đúng file/tháng/checksum/pipeline/source. Archive `REPLACE_VERSION` và retire Fact version cũ trong cùng transaction; ảnh Fact lưu source key, trip key và fingerprint. Candidate audit đọc ảnh retire để giữ lineage khi Fact nằm ngoài bảng active.
- QA batch 7: lỗi division-by-zero sau archive rollback Fact và archive event; chạy lại retire đúng 4.911 Fact version 1, giữ nguyên count và hash identity của version 2 (4.449) và version 4 (127). dbt lifecycle/parity 2/2 PASS. Fixture giữ trạng thái FAILED có chủ đích vì đây là kiểm thử Fact, chưa publish OLAP/watermark cho batch 7. Runner thường vẫn chặn replacement.
- Bổ sung OLAP rebuild theo ngày pickup và tháng nguồn bị tác động, tính lại trên active Fact và loại nhóm rỗng. Chạy batch 7 QA: OLAP 3/3 PASS; EXCEPT ALL so grain và trip count với Fact bằng 0 cho cả daily, monthly zone và monthly route, mỗi bảng cộng 4.576 trip. Chưa kiểm retry publication sau lỗi giữa các model trong runner.

## Blocker 3 — Historical parity (PARTIAL)

- So trực tiếp Parquet 2024-05 version 5 đã xác minh checksum với legacy registry restore. Mẫu giới hạn 512 dòng trên mỗi row group, đủ 4/4 row group. Khớp lineage, raw-exact và candidate fingerprint 2.048/2.048. Chỉ dùng bảng tạm với statement timeout và temp file limit.
- Sau retire 4.911 dòng Fact, candidate QA và registry đều 10.129 dòng, EXCEPT ALL hai chiều 0 khác biệt. DUP-002 còn thấy sau retire ở version 1 (128 dòng) và version 4 (129 dòng).
- Legacy thật chỉ có version 5, 3.723.833 dòng, 0 cặp cross-file. Bản restore thiếu các nhánh DDS/DQ persisted của version 5, nên chưa thể có đối chiếu candidate persisted toàn bộ hoặc chứng minh DUP-002 lịch sử thật. Muốn có bằng chứng đầy đủ cần rebuild immutable source đã duyệt trong môi trường đủ dung lượng; chưa triển khai.

## Blocker 4 — Disk forecast và giới hạn pilot (FIN-15 BLOCKED)

- Lần đo mới: D trống 43.268.763.648 byte (~40,30 GiB), tổng 125.829.115.904 byte, dự phòng 25% là 31.457.278.976 byte. Ngân sách trên mức dự phòng 11.811.484.672 byte (~11,00 GiB), hoặc ~9,00 GiB nếu giữ thêm biên dừng 2 GiB đề xuất. VHDX logical 60.863.545.344 byte. Database live 6.398.218.723 byte, legacy registry 1.822.900.224 byte và WAL directory 1 GiB đã nằm trong dung lượng dùng. Giữ ngân sách chỉ cho tăng trưởng.
- Baseline staging một file ~4,97 GiB, còn ~4,03 GiB sau biên dừng đề xuất cho Fact/DQ/index/WAL/temp/work. Chưa đủ dữ kiện đo peak để xác nhận phần đó. Hồ sơ đề xuất phép thử tối đa 20.000 dòng, 10 phút, cap tăng host 512 MiB, tăng database 256 MiB và work 128 MiB, cùng timeout, giới hạn temp và watchdog. Phép thử dung lượng mới chưa chạy.

## Blocker 5 — Hồ sơ owner review

- [STORAGE_PILOT_SIGNOFF.md](deployment/runbooks/STORAGE_PILOT_SIGNOFF.md) tập hợp evidence, điều kiện quyết định, phương pháp forecast, ngưỡng dừng và chỗ ghi sign-off của Project Lead/Integration và dbt Owner. Hai quyết định vẫn PENDING.
- Final verification: suite 36/36, `py_compile`, `git diff --check` PASS; dbt lifecycle/parity 2/2 và OLAP 3/3 PASS trong QA. So lại cả trip count và doanh thu qua ba grain OLAP, 0 khác biệt, mỗi bảng 4.576 trip. QA batch 7 lưu làm evidence của lần chạy có giới hạn, watermark live vẫn giữ nguyên sau xử lý batch 34.
- NO-GO pilot và Task 6. Còn cần kiểm thử publication/retry replacement trong runner, parity persisted toàn bộ, số đo/forecast ở tải pilot có headroom và quyết định của cả hai owner. Registry legacy và consumer production giữ nguyên.

## Task 1 — Recovery Runner

- Runner kiểm phiên bản, checksum và số dòng Staging trước khi bắt đầu hoặc chạy lại downstream. CLI có khóa phiên, `--retry-cleanup` độc lập và `--continue-run` để giữ parent. Child SUCCESS được commit cùng watermark và reconciliation, trước cleanup. Cleanup pending chặn file tiếp theo.
- Recovery suite 26/26 PASS, gồm PostgreSQL rollback và các regression CLI. Kiểm thử pipeline/retry thật trên database cô lập theo Task 2–3.

## Task 2 — Restore và database QA (PASS với backup mới)

- Backup ghi trong checkpoint cũ không có tại đường dẫn đã ghi. Tạo backup custom mới tại `D:\DockerBackups\nyc_taxi\nyc_taxi_task2_20261119.dump`, 473.028.105 byte, SHA-256 `2061A8C8CC377169D1D8735A93043CC71CBD8742A329AD9819B4DC2E5EC90F51`. Restore `--exit-on-error --single-transaction` vào `nyc_taxi_qa_restore_20261119` thành công. Nguồn và bản restore khớp 25 batch, max ID 34 và 3.723.833 dòng fingerprint registry.
- Database `nyc_taxi_qa_fixture_20261119` có schema từ backup, nhập lookup trước geometry theo FK, 265 zone và 263 polygon. Migration 006 và control functions cài vào fixture DB, registry fixture ban đầu rỗng. Sửa lỗi parse parity test khi candidate tắt, dbt seed 5/5 PASS trên fixture DB. Hai database QA dùng cùng PostgreSQL instance, tách theo database.

## Task 3 — End-to-end fixture (PASS trong phạm vi fixture)

- Mage → dbt → reconciliation → OLAP → watermark → cleanup chạy trên 2 file sample, mỗi file 5.000 dòng. Batch 1 (2024-05): 4.911 Fact, 89 business DQ. Batch 2 (2025-05): 4.449 Fact, 551 business DQ. Hai file có 0 technical và duplicate reject tự nhiên, reconciliation row/amount delta bằng 0. dbt mỗi lượt thành công 19 models, 165 tests, 3 OLAP models.
- Lượt đầu bắt lỗi CBD null vật lý của v001. Sửa nguồn fingerprint để nhận đúng cột vắng mặt trên schema v001. Đồng thời chuyển dim_date sang incremental giữ ngày của Fact lịch sử qua cleanup. Resume batch 1 thành công, rồi tiếp tục cùng parent 1.
- Batch 2 giả lập lỗi xóa work files: batch và child vẫn SUCCESS, cleanup FAILED, lệnh tiếp tục bị chặn. `--retry-cleanup 2` đưa cleanup về SUCCESS, Raw/Canonical rỗng.
- Replay Parquet 2024 checksum/version cũ qua batch 3 sau cleanup: số Fact và distinct key giữ 9.360, hash ánh xạ source key → trip_key giữ `906cb3c8cc56a29d82deb7aff0c6a567`, business DQ giữ 640, duplicate DQ giữ 0. Log ở `evidence/run_logs/storage_qa/task3_*.log`.

## Task 4 — Historical parity và replacement (PARTIAL)

- Registry bản restore có 3.723.833 dòng, toàn bộ thuộc version 5 của 2024-05, không có match pair. Đọc 512 dòng đầu trực tiếp từ Parquet gốc sau khi xác minh SHA-256, dùng SQL fingerprint model thật trên bảng tạm: source key / row / version / checksum 512/512, raw-exact 512/512, candidate fingerprint 512/512. Ba nhánh persisted chưa có trong bản restore, nên chưa có đối chiếu candidate vật lý toàn bộ lịch sử.
- Database fixture sau replacement: candidate và legacy registry cùng 10.129 dòng, EXCEPT ALL hai chiều 0 sai khác cả lineage, fingerprint và disposition. Warning view sau cleanup thấy DUP-002 cho 128 dòng version cũ và 129 dòng version mới, kể cả DQ. Dòng trùng cùng file thứ 129 là occurrence 2 và vào duplicate quarantine.
- Variant cùng tên/khác checksum bị chặn khi chưa duyệt. Sau phê duyệt QA, runner build version mới, giữ 4.449 Fact của 2025, nhưng vẫn giữ 4.911 Fact của version 2024 cũ cùng 127 Fact mới. Replacement archive/deactivate chưa đạt. Runner được bổ sung chặn trước Staging khi phát hiện version đã SUCCESS khác checksum cho cùng nguồn cho đến khi xử lý được retirement và lịch sử. Kết quả replacement QA là bằng chứng lỗi scope, không phải sign-off hay lý do cutover.
- Kiểm tra chốt chặn sau sửa trên QA: batch 6 kết thúc FAILED với mã `SOURCE_VERSION_REPLACEMENT_BLOCKED`; Raw/Canonical vẫn rỗng. Log trong `evidence/run_logs/storage_qa/task4_guard.log`. Chế độ archive đã chạy thật, guard/deactivate đầy đủ của materialization với replacement vẫn cần chứng minh.

## Task 5 — FIN-15 và sign-off (NO-GO)

- FIN-01 kiểm metadata Parquet local: 24 dòng catalog, 24 tháng khác nhau, 24 file hiện hữu, footer tổng 89.892.322 dòng. Chưa nạp phạm vi này vào warehouse.
- Ổ D: tổng 125.829.115.904 byte, trống 43.269.160.960 byte (~40,30 GiB), mức dự phòng 25% là 31.457.278.976 byte (~29,30 GiB), ngân sách dùng thêm 11.811.881.984 byte (~11,00 GiB). VHDX logical 60.863.545.344 byte. Riêng baseline Raw/Canonical một tháng khoảng 4,97 GiB, còn khoảng 6,03 GiB cho phần tăng Fact/DQ, WAL, temp, work và overhead. Chưa có số đo peak tải thật có thể chứng minh mức này đủ.
- Tham chiếu QA sau retry/replay/replacement: database 58.790.371 byte, Fact 12.369.920 byte cho 9.487 dòng (heap, index và TOAST), business DQ 638.976 byte, registry 6.979.584 byte. Đây là kích thước cuối của fixture, không phải peak/forecast đáng tin cậy cho tháng thật. FIN-15 BLOCKED.
- Hồ sơ sign-off đã có kết quả restore, fixture, parity, replacement và dung lượng tại các mục Task 1–5. Project Lead/Integration cần quyết định ngân sách pilot, baseline/backup và xử lý batch 34. dbt Owner cần xác nhận retirement theo version, parity persisted lịch sử, DUP-002 và kế hoạch benchmark index. Hai quyết định vẫn PENDING tại đặc tả V2.
- Kết luận: Task 1 PASS ở QA, Task 2 PASS bằng backup mới, Task 3 PASS ở fixture, Task 4 PARTIAL, Task 5 PARTIAL với FIN-15 và chữ ký BLOCKED. NO-GO cho reset database local, pilot và Task 6. Giữ lại hai database QA và backup mới làm bằng chứng. Không đổi Design Freeze hoặc consumer production.
- Kiểm tra cuối: suite 36/36 PASS, `py_compile` và `git diff --check` PASS. Dừng ở Task 5 theo yêu cầu.

## P2 — retry, cleanup và parent gates 2026-11-19

- Thêm migration additive `006_backfill_recovery_and_cleanup.sql`: retry downstream chỉ từ `FAILED/DOWNSTREAM_FAILED`, giữ advisory lock single-writer và từ chối khi Staging mất/trộn batch; cleanup có checkpoint `PENDING/RUNNING/SUCCESS/FAILED`; global reconciliation view tổng hợp child/reconciliation/required steps/cleanup. Migration không xóa hoặc cutover registry.
- Runner có thể gọi retry gate cho batch FAILED; cleanup mặc định vẫn khóa và chỉ bật khi đồng thời có `--enable-cleanup` + `ALLOW_STAGING_CLEANUP=1`. Cleanup kiểm batch SUCCESS, khóa writer, từ chối foreign batch, checkpoint trước TRUNCATE và cho phép retry work-file cleanup. `--no-cleanup` vẫn chỉ dùng verification một file và không finalize parent.
- Parent SUCCESS fail-closed: đúng 24/24 child, DDS và OLAP/BI validation SUCCESS, 24 reconciliation, 24 cleanup, mọi required step SUCCESS và global balance. Chưa có bằng chứng 24/24 hoặc BI nên parent production vẫn không thể PASS.
- `python -m unittest discover -s tests -p 'test_backfill_*py' -v`: 11/11 PASS; `py_compile` và `git diff --check` PASS. Đây mới là unit/static contract; tiếp theo chạy migration/function trong transaction rollback và fixture lifecycle/replacement, không áp dụng migration production.

## P2 — fixture recovery, golden và global gate 2026-11-19

- Migration 006 và `resume_failed_downstream` đã chạy trên PostgreSQL thật trong transaction rollback: batch `FAILED/DOWNSTREAM_FAILED` với một Staging batch hợp lệ chuyển về `DBT_RUNNING`, các downstream step về PENDING; Staging trộn batch bị từ chối. Không giữ migration hoặc fixture row trong database production.
- Global reconciliation view đã kiểm bằng 24 child giả lập trong transaction rollback: đúng 24 reconciliation, 24 child SUCCESS, 24 cleanup SUCCESS, required steps không lỗi, tổng rows đúng và balance đạt. Đây không phải bằng chứng 24 file thật hay parent production.
- Golden fingerprint chạy qua SQL model thật: vector schema 2024 không CBD và 2025 có CBD khớp bốn SHA-256 cố định. Candidate/history/DUP-002 fixture tiếp tục PASS. Lifecycle contract xác nhận guard, baseline archive và stale/delete đều bị giới hạn bởi processing file versions; chưa chạy custom materialization end-to-end với Fact thật.
- Kết quả: backfill/recovery/cleanup 15/15 PASS; fingerprint 2/2 PASS; lifecycle scope 2/2 PASS; `dbt parse` 1.8.7 PASS; validation hai Parquet fixture 2024/2025 PASS; `git diff --check` PASS. Cleanup production vẫn feature-gated; registry cũ chưa đổi/xóa; disk/sign-off gate không đổi.

## P2 — FIN fixture độc lập và siết parent gate 2026-11-19

- FIN-02/03 fixture chạy SQL reconciliation model thật trên hai file version, gồm technical/business/duplicate branches và amount eligibility: PASS. FIN-04 duplicate ranking CTE thật xác nhận survivor là `source_row_number` nhỏ nhất trong cùng file: PASS. FIN-10 control lifecycle/version approval/retry idempotent chạy rollback: PASS. FIN-12 hai OLAP model thật khớp revenue, tip rate và ratio-of-sums speed: PASS.
- Global parent gate được siết: required step thiếu cũng là failure (đúng 7 step), phạm vi phải đúng `2024-01..2025-12`, tổng source rows đúng footer 89.892.322. Thêm hàm ghi DDS hoặc OLAP/BI validation chỉ cho parent 24 file và bắt buộc actor + evidence không rỗng; không tự động coi code/test là BI sign-off.
- Sau thay đổi: backfill suite 16/16 PASS; từng fixture reconciliation/KPI/duplicate/control lifecycle 1/1 PASS; `py_compile` các file thay đổi và `git diff --check` PASS. Các fixture PostgreSQL đều bọc transaction rollback. Chưa chạy Fact custom materialization end-to-end, replay sau cleanup, pilot, BI thật hoặc full 24 file.

## P2 — consumer switch registry fail-closed 2026-11-19

- Thêm consumer switch `use_fingerprint_history_candidate`, mặc định `false`. Classification và cross-file match tiếp tục dùng registry legacy theo mặc định; khi được phê duyệt mới có thể trỏ tới physical source `audit.v_trip_fingerprint_history` mà không tạo dbt graph cycle. Candidate build cũng mặc định tắt.
- `dbt parse` PASS khi đồng thời bật candidate, consumer switch và parity test; fingerprint suite 3/3 PASS và duplicate survivor fixture tiếp tục PASS. Đây chỉ chứng minh code path compile/fixture; chưa có parity dữ liệu legacy thật, V2 vẫn thiếu hai owner ký, nên không bật switch, không cutover và không xóa registry.

## P2 — replay authorization 2026-11-19

- Thêm đường replay opt-in cho đúng một catalog month/file version. Sau khi discovery và checksum thật trả `SKIP_ALREADY_PROCESSED`, replay chỉ được chuyển sang `REPROCESS_IDEMPOTENTLY` khi CLI cung cấp đủ file version, actor, reason và file version khớp chính xác checksum/catalog vừa verify.
- Migration 006 thêm `authorize_file_replay`: khóa attempt, yêu cầu identity bất biến khớp `source_file_version`, yêu cầu reconciliation history của một batch SUCCESS, chuyển attempt sang VALIDATED và ghi audit `FILE_REPLAY_AUTHORIZED`. PostgreSQL rollback fixture xác nhận status/audit; mismatch bị unit gate từ chối.
- Backfill suite sau replay: 18/18 PASS; riêng PostgreSQL recovery/global/replay 3/3 PASS; `py_compile` và `git diff --check` PASS. Chưa chạy replay Parquet end-to-end sau cleanup vì disk/cleanup/pilot gates chưa đạt; FIN-09 vẫn PARTIAL, không PASS.

## P2 — disk và approval preflight fail-closed 2026-11-19

- Runner giờ bắt buộc `--expected-peak-bytes > 0` trước mọi ingestion mới và từ chối nếu free space dự kiến sau peak thấp hơn 25% tổng ổ chứa project/Docker data. Resume downstream không tạo Staging mới nên không đi qua gate forecast này.
- Full đúng 24 tháng còn bắt buộc `ALLOW_24_MONTH_BACKFILL=1` và một evidence file nằm trong project có cả Project Lead/Integration và dbt Owner ở trạng thái APPROVED. Tài liệu hiện vẫn PENDING nên full run bị khóa theo thiết kế.
- Backfill suite 21/21 PASS sau các gate; `py_compile` và `git diff --check` PASS. Đây là kiểm thử logic preflight, không thay FIN-15: chưa có peak đo từ pilot nên chưa có giá trị forecast được phê duyệt để chạy pilot/full.

## P2 — đóng race single-writer 2026-11-19

- `control.start_batch` của pipeline production và cleanup/retry giờ dùng cùng advisory writer lock. Cleanup còn từ chối nếu Staging có foreign rows hoặc tồn tại production batch khác chưa terminal; vì vậy batch cũ STAGED/RUNNING phải được giải quyết trước, không thể bị TRUNCATE lách qua.
- Full parent không tự finalize ngay sau child 24 vì DDS/BI evidence chỉ có sau đó; thêm `--finalize-run` riêng, vẫn chịu toàn bộ gate SQL. Backfill suite 23/23 PASS, modified control functions compile và lifecycle fixture PASS trong rollback; không áp dụng function/migration lên production.

## P2 — checkpoint dừng do external gates 2026-11-19

- Final verification của toàn bộ test mới ở root `tests/test_*.py`: 32/32 PASS. `dbt parse` 1.8.7 PASS khi bật candidate + consumer switch + parity test; `git diff --check` PASS. PostgreSQL fixtures đều rollback; không áp dụng migration 006, không cleanup, không replay/pilot/backfill và không cutover registry.
- Metadata chỉ đọc cuối: database 6.398.095.843 byte; D trống 43.780.063.232 byte (~40,77 GiB), C trống 74.367.348.736 byte (~69,27 GiB). Batch production 34 vẫn `DBT_RUNNING` trong khi Raw và Canonical Staging đều 0 row, nên retry downstream hợp lệ bị gate từ chối; phải điều tra/resolve có phê duyệt, không tự phục hồi giả.
- `dds.fact_taxi_trip`, `dq.business_quarantine`, `dq.duplicate_quarantine` vẫn chưa tồn tại. Vì vậy không thể chạy parity legacy thật, custom Fact lifecycle end-to-end, DDS/BI validation hoặc replay thật. Candidate/switch vẫn mặc định tắt và registry legacy giữ nguyên.
- V2 vẫn PENDING ở cả Project Lead/Integration và dbt Owner; FIN-15 vẫn BLOCKED vì chưa có pilot peak forecast. Các việc còn lại đều cần ít nhất một trong: hai owner ký, migration production được phê duyệt, xử lý batch 34, thêm headroom/forecast pilot, rồi pilot một file và các gate tiếp theo. Không còn nhóm implementation/fixture an toàn độc lập nào đã xác định mà chưa thực hiện trong lượt này.

## P2 — tiếp nối lifecycle, reconciliation và parent run

- Fact guard/archive xác định file version từ Raw Staging theo `load_batch_id` nếu có. Baseline archive cũng giới hạn theo scope thay vì quét và ghi lại toàn bộ Fact. Chưa chạy fixture lifecycle archive/guard hoặc replacement hai version.
- Runner validate reconciliation trước OLAP và ghi history/counters trong cùng transaction với `commit_batch_success` sau OLAP. Parent/child records được tạo bằng một câu SQL. Child lấy file version từ attempt theo batch, không lấy version lớn nhất theo tháng. Finalize SUCCESS của run thử một file đòi child/batch SUCCESS và reconciliation history tương ứng. Gate parent 24/24, cleanup và retry FAILED vẫn chưa được triển khai đầy đủ.
- dbt parse 1.8.7 PASS với candidate bật và file version fixture 1. Fixture SQL parity/DUP-002 1/1 PASS. Runner safety 6/6 PASS gồm lỗi OLAP, lỗi commit mô phỏng và thiếu child hợp lệ. `py_compile` và `git diff --check` PASS. Chưa chạy logic parent SQL trên database thật hoặc các test end-to-end mới.
- Metadata chỉ đọc cho thấy `dds.fact_taxi_trip`, `dq.business_quarantine`, `dq.duplicate_quarantine` đều chưa tồn tại trong database hiện tại. Chưa thể chạy parity legacy trên ba nhánh hiện hữu. Database 6.397.620.707 byte, VHDX 60.829.990.912 byte, C 69,29 GiB, D 40,77 GiB. Không áp dụng migration hoặc chạy ingestion/pilot. Registry cũ giữ nguyên và mọi gate/FIN vẫn theo P1.
- Tiếp tục bằng fixture dbt 2024/2025 đủ ba nhánh để kiểm chứng fingerprint, DUP-002 persistence, lifecycle và reconciliation. Hoàn thiện retry có khóa và xác nhận staging trước khi khôi phục FAILED; sau đó global reconciliation, parent/child 24 và cleanup retry. Cutover/cleanup production cần sign-off và gate dung lượng.

- P2 tiếp nối: view ứng viên `v_trip_cross_file_warning` trả DUP-002 cho cả hai file version từ bảng match đã lưu và giữ disposition. Fixture PostgreSQL PASS cho hai version, ba nhánh và hai loại match trên cùng dòng. Parity SQL PASS với fixture khớp và phát hiện hai dòng sai lệch sau khi đổi lineage. Parity hiện chặn cả trường hợp baseline rỗng. Chưa chạy parity legacy hoặc bật view trên database thật.

## P2 — fingerprint candidate 2026-11-19

- Thêm model `dbt_project/models/audit/v_trip_fingerprint_history.sql` (mặc định disabled), UNION ALL ba physical source Fact/business DQ/duplicate DQ; source declarations riêng tránh vòng phụ thuộc dbt. Registry và consumers hiện tại không đổi; chưa triển khai view vào database.
- Thêm test dbt `fingerprint_history_candidate_parity.sql`, chỉ bật khi candidate được bật và bắt buộc chỉ định file version; dùng EXCEPT ALL hai chiều so source key, lineage, fingerprint và disposition, bao gồm phát hiện duplicate. Test chưa chạy trên dữ liệu thật; zero mismatch chưa đủ để chấp nhận cutover (còn DUP-002, reconciliation, migration và lịch sử cũ).
- `python -m unittest discover -s tests -p test_fingerprint_candidate_sql.py -v`: 1/1 PASS. Test render model thật và chạy union/bytea trên ba bảng tạm PostgreSQL trong transaction rollback. Chưa chứng minh dữ liệu legacy, golden 2024/2025 hay cross-file warning; gate/FIN không đổi.
- Bước tiếp: chạy dbt parse candidate và fixtures tương đương legacy/cross-file; giải quyết warning lịch sử cho mọi nhánh, parity theo version và completeness trước cutover. Đặc biệt candidate chỉ bao gồm dòng đã có Fact/DQ; registry hiện có thể chứa lịch sử không có mặt ở ba nhánh, nên không được xóa/đổi consumer trước khi chứng minh và xử lý chênh lệch.

## P2 — checkpoint an toàn 2026-11-19

- Đã sửa `deployment/scripts/run_24_month_backfill.py`: chặn cleanup mọi trường hợp cho đến khi gate an toàn được chứng minh; yêu cầu `--no-cleanup`, chặn xử lý nhiều file khi cleanup chưa an toàn; nhánh không có file xử lý fail thay vì giả lập downstream SUCCESS/watermark; thông báo số file đúng theo scope. Không chạy migration hoặc thay đổi database.
- Thêm `tests/test_backfill_safety_gates.py`: 3/3 PASS (`python -m unittest discover -s tests -p test_backfill_safety_gates.py -v`); `python -m py_compile deployment\scripts\run_24_month_backfill.py tests\test_backfill_safety_gates.py` PASS. Đây là unit test fail-closed, không chứng minh workflow end-to-end. FIN/gate vẫn như P1.
- Đo cuối: C trống 69,33 GiB, D trống 40,77 GiB; database 6.397.587.939 byte (không đổi so baseline), VHDX 60.829.990.912 byte (không đổi so sau P0). Không chạy pilot/ETL. Proposal V2 chưa có sign-off; không bật cleanup production hoặc xóa registry.
- Còn lại: thay registry bằng view và migration không phá hủy, chứng minh tương đương đầy đủ trước khi chuyển consumer hoặc xóa dữ liệu; sửa lịch sử cross-file/DUP-002, lifecycle, reconciliation, retry/resume và parent run; fixture/golden/failure tests, pilot có headroom, BI và full backfill. Cleanup hiện cố ý không khả dụng; không dùng runner để nạp chính thức.
- Tiếp tục: xem `dbt_project/models/audit/trip_fingerprint_history.sql`, `dbt_project/models/intermediate/int_taxi_trip_classified.sql`, `sql/migrations/005_storage_optimization_v2.sql` và schema Fact/DQ; chuẩn bị view song song, kiểm thử đối chiếu lineage/fingerprint đầy đủ trước bất kỳ cutover. Sau đó hoàn thiện runner và test gates theo thứ tự P1. Không xóa bảng hiện hữu.

## P1 — kiểm toán 2026-11-19

Tài liệu `STORAGE_OPTIMIZATION_FINAL_24_MONTHS.md` vẫn ghi chờ Project Lead và dbt Owner ký duyệt; Design Freeze đã APPROVED. Không coi proposal là thay thế đã được ký cho Freeze. P1 là kiểm toán tĩnh và metadata, không chạy ETL/pilot; trạng thái dưới đây không đồng nghĩa kết quả end-to-end.

**Git:** commit 1c0f342 chứa triển khai trước; bốn SQL OLAP/quarantine đang sửa chưa commit và checkpoint chưa tracked. Không sửa code ở P1. Mage 18/18 PASS trước đây chỉ là unit/integration, không phải FIN.

**Giai đoạn:** 0 PARTIAL — chưa có tháng end-to-end SUCCESS/rerun, pilot trước lỗi; 1 PARTIAL — DQ chọn cột, index/work cleanup có code, chưa chứng minh tính tương đương; 2 PARTIAL — migration source version, reconciliation, lifecycle scope và incremental có code nhưng chưa chứng minh gates; fingerprint history sai thiết kế; 3 NOT STARTED theo gate — cleanup có code nhưng chưa được kiểm thử an toàn, chưa có pilot 2024/2025 hoặc replay; 4 NOT STARTED — chưa backfill 24 file và final validation.

**Sai lệch và rủi ro:** `audit.trip_fingerprint_history` là bảng incremental một dòng/source key, không phải view UNION ALL Fact/business DQ/duplicate DQ như mục 5.2. Bảng live 1.822.900.224 byte, ước lượng thống kê 3.723.848 dòng (reltuples có thể cũ), chưa kể cross-file match; nhân tuyến tính 89.892.322 dòng khoảng 41 GiB chỉ cho registry, không phải dự báo tổng. `cross_file_fingerprint_match` và classification phụ thuộc registry này. Không xóa hay sửa bảng trong P1. Cần giải quyết lịch sử warning cho cả dòng cũ, fingerprint golden tests và dependency sau cleanup.

Runner có commit rồi TRUNCATE staging nhưng chưa chứng minh single-writer, cleanup pending/retry hoặc cách ly batch; `--resume-batch` chỉ chấp nhận STAGED/DBT_RUNNING trong `run_downstream`, trong khi lỗi chuyển FAILED; nhánh `mark_skipped_downstream_success` đánh dấu các bước downstream thành công, cần kiểm toán điều kiện skip và ngăn bypass gate. Reconciliation history được ghi trước OLAP/commit SUCCESS, cần kiểm tra successful-run semantics. Fact lifecycle có scope file version trong nhánh archive, guard và thay version cần kiểm thử. Chưa có bằng chứng parent run/global 24/24 hoặc đo WAL/temp/peak. Hai bảng staging live hiện mỗi bảng 65.536 byte; reltuples cũ không chứng minh số dòng thực tế. Trạng thái batch 2024-05 và khả năng phục hồi dữ liệu staging cần xác minh trước mọi cleanup/replay.

**FIN — chỉ PASS khi có bằng chứng tương ứng:** FIN-01 PARTIAL (proposal ghi 24 file/89.892.322 footer, chưa xác minh lại); FIN-02 PARTIAL (SQL có, chưa chứng minh theo version); FIN-03 PARTIAL (reconciliation có, chưa end-to-end); FIN-04 PARTIAL (logic có, golden test chưa chứng minh); FIN-05 PARTIAL (logic có nhưng history sai kiến trúc, historical warning chưa chứng minh); FIN-06 PARTIAL (upsert/resume có, retry và trip_key chưa chứng minh); FIN-07 PARTIAL (thứ tự có, fault test chưa chứng minh); FIN-08 PARTIAL (cleanup có, isolation/retry chưa chứng minh); FIN-09 NOT STARTED (chưa có bằng chứng replay sau cleanup); FIN-10 PARTIAL (version metadata có, approval/scope chưa chứng minh); FIN-11 PARTIAL (có schema code, golden v001/v002 chưa chứng minh); FIN-12 PARTIAL (OLAP speed đã sửa chưa test SQL/BI); FIN-13 PARTIAL (models/tests có, chưa chứng minh full DDS); FIN-14 NOT STARTED (chưa có bằng chứng BI end-to-end); FIN-15 BLOCKED (thiếu pilot và dự báo peak đáng tin cậy).

**Dung lượng:** D tổng 117,19 GiB, trống 40,77 GiB; giữ 25% tổng ổ tức 29,30 GiB, chỉ khoảng 11,47 GiB tăng thêm trước khi chạm ngưỡng. Đây là ngân sách chung cho tăng VHDX, WAL, temp, file nguồn/work, backup và dữ liệu khác; không cộng database 5,96 GiB hoặc VHDX hiện có lần nữa. Baseline tài liệu staging một tháng 5.335.392.256 B (~4,97 GiB) chỉ Raw+Canonical, chưa có Fact/DQ, WAL, temp, peak hoặc tăng VHDX. Pilot một tháng cần tối thiểu lượng staging tương đương nếu bắt đầu rỗng, peak chưa biết; pilot hai tháng tuần tự không thể dự báo đáng tin cậy, không phê duyệt theo 11,47 GiB hiện có. Full 24 tháng: chưa đủ số đo Fact/DQ/index/WAL/temp để dự báo hợp lệ; registry hiện tại nếu tuyến tính đã riêng ~41 GiB, vượt ngân sách 11,47 GiB. Cần thêm dung lượng hoặc thiết kế/đo lại trước pilot, không tuyên bố đủ 24 tháng.

**P2 theo dependency:** (1) xác minh trạng thái batch/source version và bảo toàn backup; xin ký proposal nếu thay đổi contract; (2) thay registry bằng history interface đúng proposal, sửa cross-file warning/lifecycle scope và xác minh golden; (3) sửa runner gate, skip, reconciliation commit, cleanup locking/retry và resume FAILED; (4) fixture nhỏ cho FIN-02–13, failure injection và restore thử vào database cô lập có ngân sách disk; (5) đo pilot một file rồi hai file 2024/2025 sau khi đủ headroom, so forecast peak; (6) chuẩn bị parent/checkpoints/BI và chỉ sau gate mới chạy backfill. Kiểm thử tĩnh, metadata và fixture giới hạn có thể chạy với disk cap theo dõi; restore thử và pilot cần dự trù dung lượng thực tế và phê duyệt. Mọi xóa/reset/di chuyển VHDX, database/volume hoặc dữ liệu persistent cần phê duyệt; không chạy pilot/full backfill trước khi được phép.

## Giai đoạn A — 2026-11-19

- Worktree sạch; thay đổi phiên trước đã nằm trong commit `1c0f342` (baseline trước đó `3ff7436`).
- Đã đọc README, kế hoạch dự án và Design Freeze. Docker engine hiện chưa chạy; trạng thái database/pilot chưa được xác nhận lại.
- Pilot trước thất bại ở hai model quarantine do ký tự `+` đầu file; PostgreSQL ngắt kết nối khi Fact lifecycle chạy. Chưa có bằng chứng hoàn thành pilot hoặc 24 tháng.
- Phát hiện OLAP tính trung bình row speed, trái yêu cầu ratio-of-sums. Cần sửa và kiểm thử.
- Cần rà runner, lifecycle, incremental, reconciliation, recovery và dự báo dung lượng trước khi chạy pilot; không reset database/volume.

## Giai đoạn B/C — checkpoint chưa hoàn tất

- Đã sửa lỗi cú pháp đầu hai model quarantine và công thức speed ở hai OLAP model về `SUM(distance) * 3600 / SUM(duration)` trên tập eligible.
- Mage unit/integration: 18/18 PASS ngày 2026-11-19. Đây không phải kiểm thử database, dbt hay idempotency end-to-end.
- Chưa xác nhận đúng đắn của incremental/lifecycle, cross-file warning, reconciliation, retry/resume hoặc cleanup. Runner hiện có `--resume-batch`, nhưng chưa chứng minh resume sau thất bại hoặc checkpoint đầy đủ; không dùng runner cho backfill chính thức trước khi kiểm thử và sửa các cổng này.
- Docker engine đang dừng; ổ C chỉ còn khoảng 2,78 GiB trống (đo 2026-11-19), thấp hơn đáng kể peak pilot trước. Vì vậy chưa chạy pilot hoặc migration; chưa có số đo pilot thành công và chưa dự báo được 24 tháng đáng tin cậy.
- Tiếp theo: kiểm tra Docker/VHDX và dung lượng trống; xác định nguyên nhân PostgreSQL ngắt kết nối ở pilot cũ; rà và sửa runner, lifecycle và DQ theo Design Freeze; chạy fixture SQL/dbt có giới hạn, kiểm retry/idempotency; sau đó pilot tối đa hai tháng khi đủ dung lượng. Viết runbook terminal sau khi các cổng thực sự hoạt động.
- Chưa nạp và chưa kiểm chứng đủ 24 tháng. Không xóa database hay volume trong giai đoạn này.

## P0 — di chuyển Docker disk image (PASS theo kiểm tra sau migration)

- Kiểm tra 2026-11-19: Docker Desktop 4.99.0; Docker engine và WSL docker-desktop đang chạy. VHDX `D:\DockerDesktopWSL\disk\docker_data.vhdx` 60.829.990.912 byte, LastWriteTime thay đổi từ 07:07:27 sang 07:09:11 sau khi khởi động container. VHDX tại đường dẫn cũ trên C không còn tồn tại. Đây là bằng chứng hoạt động trên D, không chỉ dựa vào giao diện.
- PostgreSQL healthy, Mage Up sau khi khởi động lại hai container đang dừng; database `nyc_taxi` 6.397.587.939 byte, `control.etl_batch` 25 dòng, max batch_id 34; trùng baseline. Volume `nyc_taxi_bi_postgres_data` vẫn có CreatedAt 2026-11-16T09:52:18Z. Các chỉ số này không chứng minh đối chiếu từng dòng dữ liệu.
- C còn 69,35 GiB; D còn 40,77 GiB khi đo. Không cần dọn VHDX cũ ở đường dẫn C vì file không còn. Không xóa hay đổi tên VHDX, không chạy ETL/dbt/backfill. Backup logical trước migration vẫn tại `D:\DockerBackups\nyc_taxi\nyc_taxi_20261119_065153.dump`; chưa restore thử.
- P0 PASS cho migration và kiểm tra baseline giới hạn; các bước tối ưu 24 tháng, thử phục hồi backup và kiểm tra đầy đủ dữ liệu chưa được thực hiện.

- Kiểm tra live 2026-11-19 06:52: Docker Desktop cài theo user là **4.83.0** (Docker Engine/CLI 29.6.2). Bản này thấp hơn 4.92.0/4.98.0 có sửa lỗi migration; cần cập nhật Docker Desktop trước khi thử lại. Release notes chính thức hiện có 4.99.0 (2026-11-09); 4.98.0 sửa lỗi chuyển disk image thất bại khi copy quá 5 phút.
- Đã tạo logical backup độc lập, chỉ đọc: `D:\DockerBackups\nyc_taxi\nyc_taxi_20261119_065153.dump`, 473.028.099 byte, custom format; `pg_restore --list` PASS; SHA-256 `6F66F624A30FED880EE3330AB3A4AEB5A2723D822D50DBB2FDE47A1A21EF90F4`.
- Bằng chứng disk đang dùng: trong lúc `pg_dump`, VHDX C cập nhật tới 06:52:44; VHDX D vẫn đứng ở 06:30:08. Hai file cùng kích thước 60.795.387.904 byte. Backend log ghi cấu hình hiệu lực `wslDataFolder=<HOME>\AppData\Local\Docker\wsl`; D chỉ xuất hiện trong yêu cầu migration thất bại. Kết luận: C là disk live; D là bản copy cũ, chưa được dùng.
- D trống 40,93 GiB, không đủ tạo thêm bản copy ~56,62 GiB khi bản D cũ còn tồn tại. Phương án chờ phê duyệt: sau khi update và backup đã xác minh, quit Docker Desktop, `wsl --shutdown`, xác nhận không còn Docker/WSL process, rồi chỉ xóa bản copy cũ `D:\DockerDesktopWSL\disk\docker_data.vhdx`; tuyệt đối giữ bản live trên C. Sau đó khởi động lại từ C, kiểm tra dữ liệu, và dùng Settings > Resources > Advanced > Disk image location để migration chính thức.
- C trống 12,54 GiB; D trống 40,93 GiB tại thời điểm kiểm tra. PostgreSQL và Mage đang Up; PostgreSQL healthy; volume `nyc_taxi_bi_postgres_data` vẫn có CreatedAt `2026-11-16T09:52:18Z`. Không chạy ETL/dbt/pilot và chưa xóa/đổi tên VHDX. P0 chưa PASS.

- Kiểm tra lại 2026-11-19: Docker backend log lúc 06:31:33 ghi `wslDataFolder` là `%LOCALAPPDATA%\Docker\wsl` (ổ C); lúc 06:31:59 giá trị D chỉ là thay đổi giao diện cần restart. File C có LastWriteTime 06:42:47, file D 06:30:08. Chưa chứng minh Docker độc lập với C; không được xóa/đổi tên VHDX nào. `Get-DiskImage` trả Attached=False cho cả hai, không đủ chứng minh WSL không dùng file.
- PostgreSQL healthy; `nyc_taxi` 6.397.587.939 byte, 25 batch, max ID 34, volume `nyc_taxi_bi_postgres_data` giữ nguyên so với baseline. C trống 12,75 GiB, D trống 40,93 GiB. P0 chưa PASS.

- Kiểm tra 2026-11-19: Docker engine hiện đã chạy; nguyên nhân từng dừng chưa xác định. PostgreSQL container healthy. Không chạy ETL/dbt/pilot.
- VHDX hiện tại: `C:\Users\PC\AppData\Local\Docker\wsl\disk\docker_data.vhdx`, kích thước logic 60.795.387.904 byte (~56,62 GiB). C còn 2,74 GiB; D còn 91,00 GiB. D đủ chỗ cho bản di chuyển hiện tại nhưng phải tiếp tục theo dõi headroom.
- Baseline trước di chuyển: volume `nyc_taxi_bi_postgres_data`, driver local, tạo 2026-11-16T09:52:18Z; database `nyc_taxi` 6.397.587.939 byte, 25 batch, batch ID lớn nhất 34.
- P0 chưa PASS: cần di chuyển bằng Docker Desktop Settings tới `D:\DockerData`, sau đó kiểm tra lại volume, PostgreSQL, counts và dung lượng C/D. Chưa xóa VHDX cũ, volume hoặc database.

