# Rà soát hiệu năng dbt — 2026-11-19

## Kết luận và phạm vi

- Nhánh hiện tại: `feature/control-metadata-watermark`, commit `c679360`.
- Nhánh so sánh: `origin/feat/fix-dbt`, commit `5fcf661`, fetch từ origin ngày rà soát. Đây là một commit nối tiếp trực tiếp nhánh hiện tại.
- **Kỳ vọng feat/fix-dbt chạy nhanh hơn**, đặc biệt giai đoạn tests và các consumer của classification. Chưa có benchmark cùng một tháng trên hai nhánh nên chưa thể đưa hệ số tăng tốc hoặc thời gian hoàn tất.
- Docker engine đang tắt lúc kiểm tra. Không có query plan, wait event hoặc dung lượng database live mới. Không chạy ingestion, migration hay sửa logic ETL/dbt.
- `target/run_results.json` hiện chỉ chứa lượt seed 5/5; không phải kết quả full-month dbt. Các lượt 165 tests thành công trong evidence là fixture nhỏ.

## So sánh thay đổi

| Điểm | Nhánh hiện tại | feat/fix-dbt | Tác động dự kiến |
|---|---|---|---|
| int_taxi_trip_fingerprinted | view | table, unique index source key, hai index fingerprint/version, ANALYZE | Tránh tính lại JSON serialization và SHA256 ở mỗi consumer |
| int_taxi_trip_classified | view | table, unique index source key, index version/disposition, ANALYZE | Tái sử dụng window sort, lịch sử match và DQ policy giữa Fact/DQ/tests |
| Fact temporary source/new keys | Không tạo index/ANALYZE | Thêm unique index source key và ANALYZE cả hai bảng | Cải thiện join plan; đổi lại chi phí dựng index |
| dim_date | UNION ALL pickup/dropoff, đọc accepted hai lần | Một aggregate lấy cả min/max | Giảm đọc accepted và dữ liệu trung gian |
| Cleanup | Dọn staging sau SUCCESS theo gate | Dọn thêm hai work table trong cùng cleanup | Giới hạn tích lũy workspace giữa tháng; lỗi vẫn giữ dữ liệu |

Đây là đổi CPU và I/O tạm lặp lại lấy dung lượng work table/index/WAL. Tạo view nhanh không chứng minh xử lý dữ liệu nhanh: dữ liệu chỉ được tính khi view bị đọc. Hai table mới vẫn dựng lại toàn bộ staging hiện có; không có filter batch sớm. Comment “batch workspace” không tự bảo đảm staging chỉ chứa một batch.

Không bỏ qua hai model mới khi resume trên một batch mới. Bảng có thể còn thuộc staging/version cũ. Chỉ dọn workspace sau tests, reconciliation, OLAP và commit SUCCESS theo runner; chạy dbt độc lập không tự thực hiện cleanup của runner.

## Bằng chứng runtime và “test 141”

Nguồn: `dbt_project/logs/dbt.log` tại thời điểm rà soát.

- Dòng 9258: `141 of 176` là `file_month_and_cbd_consistency`, trạng thái SKIP.
- Dòng 28815/28935: `141 of 165` là `unique_dim_vendor_vendor_key`, PASS 0,05 giây; các lượt fixture khác cũng rất nhanh.
- Lượt build bắt đầu 18:38:43 có các test fingerprint bắt đầu khoảng 18:39:48 (94–98/176), nhưng phần log đã lưu không chứa kết quả hoàn tất tương ứng trước invocation kế tiếp. Không đủ để suy ra chúng chạy liên tục trong khoảng trống log.
- Dòng 18885: dim_date 251,51 giây, 7.827 dòng đầu ra.
- Dòng 19369: trip_fingerprint_history 599,76 giây, INSERT 3.723.833 dòng.
- Dòng 19457: fact_taxi_trip ERROR sau 901,28 giây; dòng 19506–19509 ghi `server closed the connection unexpectedly`.
- Dòng 19489: lượt run kết thúc sau 1.153,83 giây (~19 phút 14 giây), thất bại. Đây không phải thời gian chạy xong tháng. Các model chạy song song nên không cộng các thời gian node thành tổng.
- Cùng lượt còn có syntax error tại dấu `+` ở business/duplicate quarantine. Code hiện tại không còn dấu này ở các vị trí model tương ứng; không coi lỗi cũ là lỗi chắc chắn còn tồn tại ở HEAD.

Số 141 không phải định danh test ổn định. Muốn xác định nghẽn cần unique_id/name của node đang RUN mà chưa có kết quả, invocation tương ứng và query/wait state PostgreSQL. Mất kết nối server cũng chưa đủ chứng minh OOM hay hết disk; cần log container/PostgreSQL và trạng thái hệ điều hành.

## Các nút thắt và hướng xử lý theo ưu tiên

### P0 — Chuỗi view lặp lại công việc đắt

`stg_yellow_taxi → enriched → fingerprinted → classified → accepted` đang dùng view. Fingerprinted join raw theo source key, chuyển cả composite row bằng `to_jsonb(r)`, trích nhiều field và tạo hai hash. Classified thực hiện row_number/first_value theo file SHA và hash, nhiều GROUP BY/DISTINCT/UNION, join lịch sử và lateral aggregate policy theo từng dòng. Fact, quarantine, dim_date, registry và nhiều tests đọc lại chuỗi này. CTE trong một statement không phải cache dùng chung giữa các query dbt.

Ưu tiên dùng hai materialization của feat/fix-dbt làm ứng viên benchmark. Đo thời gian dựng table/index cùng thời gian downstream để đánh giá tổng, không chỉ nhìn một test.

### P1 — Dòng quá rộng và xử lý JSON

`stg_yellow_taxi` mang raw_payload; fingerprinted còn thêm raw_values; các tầng dùng `e.*`, `j.*`, `f.*`, `d.*`. Hai table mới có thể lưu JSON lặp lại, nhiều cột lineage, cờ DQ và hash hex. Điều này làm tăng heap/TOAST, WAL, sort/hash và disk peak.

Đề xuất projection cột rõ ràng. Tách raw_values/raw_payload sang nhánh kiểm contract, không đưa toàn bộ JSON vào bảng classification nếu downstream không cần. Đọc trực tiếp r.raw_payload thay to_jsonb(r) chỉ khi đã chứng minh contract cho mọi schema/version và giữ fallback hợp lệ. Không đổi thứ tự/type serialization hash nếu chưa qua golden vectors 2024/2025.

### P1 — Tests quét dữ liệu tích lũy

`fact_lifecycle_history.sql` dùng window, lag, distinct on và group trên toàn history, rồi đối chiếu toàn Fact. `fact_accepted_membership.sql` có hai chiều join Fact/accepted, so sánh rất nhiều cột và kiểm quarantine. `trip_classification_partition.sql` union và group source keys. Generic tests trên intermediate view cũng có thể kích hoạt tính lại; mức độ phụ thuộc projection và query plan.

Đề xuất tách kiểm tra mỗi batch và audit toàn lịch sử. Batch test phải lấy đủ tập khóa bị tác động: batch/file version đang nạp, old/new replacement versions và các khóa historical liên quan cross-file warning; chỉ lọc batch_id cơ học có thể bỏ sót lỗi. Full audit chạy theo checkpoint/lịch riêng. Giữ các cổng reconciliation, identity, membership, lifecycle và watermark; không bỏ tests để đạt số phút đẹp. Có thể gom một số not_null/flag checks thành một lần aggregate nếu giữ đủ chi tiết lỗi.

### P1 — Index và thống kê trên bảng persistent

Fact materialization hiện tạo PK trip_key và UNIQUE source_record_key, nhưng không thấy DDL index file_version_id trên Fact trong code đã rà. Reconciliation, membership và OLAP đều truy cập theo version; khi Fact tích lũy nhiều tháng, có nguy cơ đọc lại phần lớn bảng. `unique_key` của dbt incremental là cấu hình thuật toán, không tự chứng minh có physical unique index trên bảng.

Đề xuất kiểm tra pg_indexes và EXPLAIN trước khi bổ sung: Fact(file_version_id), quarantine(source_record_key)/(file_version_id), reconciliation(file_version_id), và các unique key thực tế của bảng incremental. Registry đã có index raw/candidate fingerprint + file_version_id trong migration 005: phải kiểm tra migration thực sự được áp dụng và model còn giữ chúng. Không thêm hàng loạt index vì sẽ tăng insert time, WAL và disk. ANALYZE sau bulk load cho các bảng đang thiếu thống kê; feat/fix-dbt đã làm với hai workspace và hai temporary tables.

### P1 — Lịch sử và ghi dữ liệu tăng theo tháng

`trip_fingerprint_history` vẫn ghi một dòng/source key, dùng delete+insert, giữ nhiều index. Fact archive giữ event và prior_fact JSONB cho thay đổi; Fact upsert so sánh nhiều cột và duy trì identity/history. Nhánh fix chưa loại các chi phí này. Cross-file joins có thể tạo nhiều kết quả trước DISTINCT khi cùng fingerprint xuất hiện nhiều dòng/version.

Đề xuất đo riêng registry insert, Fact source creation, upsert và history insert. Với classification chỉ cần biết tồn tại, thử EXISTS/semi-join và tập fingerprint distinct sớm; kiểm tra plan/parity trước đổi. Với cross-file audit phải giữ đúng cặp version và detection lineage. History interface thay registry hiện đã có candidate trong repo nhưng mặc định tắt; không coi là giải pháp sẵn sàng cutover trước parity, cảnh báo historical và lifecycle validation.

### P2 — Scope và concurrency

`load_batch_id` chủ yếu lọc ở Fact; staging/fingerprinting/classification và current_files reconciliation lấy staging hiện có. Nếu staging tích lũy do cleanup tắt/lỗi, mỗi tháng sẽ xử lý lại dữ liệu cũ. Runner gọi toàn bộ dbt test cho mỗi tháng. Với lịch sử tăng đều, tổng chi phí các full scan qua 24 batch có thể gần tổng 1+2+...+24 lần dữ liệu tháng, thay vì chỉ 24 lần.

Giữ single writer và xác minh staging/version trước chạy. Không thêm filter batch sớm làm mất duplicate/cross-file context. So sánh threads=1,2,4 trên cùng tài nguyên; bốn query nặng có thể cạnh tranh CPU/RAM/disk. docker-compose hiện không đặt tham số tuning PostgreSQL; chưa có SHOW settings live nên chưa kết luận giá trị thực tế. Điều chỉnh work_mem theo plan và RAM thực, không tăng toàn cục tùy ý vì áp dụng theo operation/worker.

## Đo dung lượng và benchmark tiếp theo

Không thể suy dung lượng từ số phút chạy hoặc kích thước Parquet. Cần đo cả dữ liệu giữ lâu dài và peak khi staging, workspace, Fact/history, WAL và temp cùng tồn tại.

Số liệu cũ trong OPTIMIZATION_PROGRESS.md: Raw+Canonical một tháng 5.335.392.256 B (~4,97 GiB); registry 1.822.900.224 B cho khoảng 3,72 triệu dòng. Tài liệu từng ngoại suy registry ~41 GiB/89,89 triệu dòng, chưa phải tổng 24 tháng. Các con số này là baseline lịch sử, không phải số đo lại hôm nay. Hai workspace mới chưa có full-month size.

Snapshot host lúc rà: C trống 67.620.515.840 B (~62,98 GiB); D trống 91.155.431.424 B (~84,90 GiB). Khác snapshot cũ trong progress; không lấy ngưỡng trống cũ làm hiện trạng. Docker đang tắt nên chưa xác nhận đường dẫn/allocated size VHDX và database hiện tại.

Quy trình đo đề xuất:

1. Dùng cùng immutable Parquet, trạng thái database ban đầu, lifecycle mode, batch scope, PostgreSQL resources và threads cho cả hai nhánh. Không chạy hai bản trên cùng production rồi so first load với rerun.
2. Bắt đầu mẫu có giới hạn để lấy plan/row width, sau đó một tháng đầy đủ; đo riêng first load và retry cùng version. Thêm một tháng nữa để đo tăng trưởng lịch sử và cross-file behavior.
3. Lưu manifest, run_results và logs vào thư mục riêng theo branch/invocation/phase. Runner hiện gọi seed/run/test/run OLAP nên artifacts mặc định bị ghi đè ở mỗi lệnh.
4. Ghi wall-clock từng phase/node/test, row count, amount reconciliation, source key→trip_key parity, hash, disposition, KPI và replacement behavior.
5. Lấy size heap+TOAST+index từng relation qua pg_total_relation_size, database size trước/sau, WAL directory/LSN delta, temp_bytes delta và dung lượng host/VHDX theo thời gian. Counter temp_bytes là tích lũy lượng ghi, không phải peak temp trên disk. Database size không bao gồm đầy đủ WAL hay host VHDX.
6. Xác định query nghẽn bằng pg_stat_activity, wait_event_type/wait_event, pg_blocking_pids; dùng EXPLAIN (ANALYZE, BUFFERS) cho SELECT trong môi trường benchmark có giới hạn. ANALYZE thực thi query, không chỉ ước lượng.
7. Forecast: permanent final ≈ baseline permanent + tổng delta Fact/DQ/history/index từng tháng; peak ≈ permanent tại tháng đó + staging + workspace/index + temporary relations/spill + WAL + phần file/work/backup cùng tồn tại. Không nhân peak một tháng với 24 nếu transient được cleanup. Không cộng hai lần database đã nằm trong VHDX.

## Tài liệu kỹ thuật đối chiếu

- PostgreSQL 16 EXPLAIN và yêu cầu statistics: https://www.postgresql.org/docs/16/sql-explain.html
- PostgreSQL 16 resource consumption, work_mem theo operation và hash_mem_multiplier: https://www.postgresql.org/docs/16/runtime-config-resource.html
- dbt materializations: https://docs.getdbt.com/docs/build/materializations

Ưu tiên thực hiện: benchmark feat/fix-dbt → thu gọn workspace → kiểm index persistent và test scope → tuning theo plan → đo peak và forecast. Chưa có căn cứ cam kết dbt một tháng dưới 8 phút hoặc đủ dung lượng cho 24 tháng.
