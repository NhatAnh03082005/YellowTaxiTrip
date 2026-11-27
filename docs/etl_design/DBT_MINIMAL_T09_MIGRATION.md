# Migration DDS tối giản (chưa áp dụng live)

Tài liệu này dành cho người vận hành có snapshot database, dung lượng dự phòng và quyền cắt chuyển consumer. QA T09 dùng database riêng; **không** chạy `dbt --full-refresh` trên Fact đang có dữ liệu. Contract đích là [DBT_MINIMAL_CONTRACT.md](DBT_MINIMAL_CONTRACT.md).

## 1. Tiền kiểm và dung lượng

Chụp DDL, owner/grants, views, BI queries, model dependencies, số hàng, kích thước và watermark trước khi sửa. Lưu bản backup có kiểm restore. Các truy vấn chỉ đọc:

```sql
SELECT current_database(), pg_database_size(current_database()) AS db_bytes;
SELECT n.nspname, c.relname, c.relkind, pg_total_relation_size(c.oid) AS bytes
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname IN ('dds','dq','control','audit','stage') AND c.relkind IN ('r','m','v')
ORDER BY bytes DESC;
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema IN ('dds','dq','control')
ORDER BY table_schema, table_name, ordinal_position;
SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes
WHERE schemaname IN ('dds','dq','control','audit') ORDER BY schemaname, tablename;
SELECT pg_get_viewdef('dds.v_fact_taxi_trip_analysis'::regclass, true);
```

Ước lượng đĩa trước cutover: `pg_total_relation_size` của Fact + DQ + aggregates đích, cộng Fact/DQ cũ **vẫn giữ**, temporary dbt workspace, WAL và backup. Dùng kích thước đo trên QA cùng tỷ lệ bytes/row làm lower bound, thêm headroom cho index và WAL; không lấy ước lượng fixture làm ngân sách live. Runner yêu cầu `--expected-peak-bytes` và kiểm free space trước khi nạp. Nếu không đủ chỗ cho bản đích song song, dừng và chốt ngân sách; không xóa history/registry để tạo chỗ.

## 2. Ánh xạ cũ → đích và phụ thuộc

| Vật lý cũ | Đích tối giản | Thao tác/cần kiểm |
| --- | --- | --- |
| `dds.fact_taxi_trip.trip_key`, `source_record_key`, `file_version_id`, `source_row_number` | Giữ identity/grain; PK `trip_key`, unique `source_record_key`, index `file_version_id` | Đối chiếu source key và số Fact theo từng version; không tái cấp key bằng full refresh |
| Fact role keys, timestamps, natural codes, passenger, distance, duration, amounts, eligibility, DQ fields | Giữ theo allowlist mục 4 contract; đổi `trip_distance` → `trip_distance_miles`, `reported_total_amount` → `total_amount` nếu đang ở tên cũ | So data type/numeric scale, nullability và amount parity trước cutover |
| Fact `raw_exact_fingerprint`, `candidate_trip_fingerprint`, raw JSON, metadata text, các bands/flags dư | Không còn trên Fact đích | Metadata file ở `control.source_file_version`; bands và alias ở `dds.v_fact_taxi_trip_analysis`; hash chỉ ở classification tạm |
| Business/duplicate quarantine cũ có hash và JSON | DQ tối giản giữ source identity, reason, giá trị điều tra, survivor/occurrence | Đối chiếu rejected rows và keys theo version |
| `control.file_reconciliation_history` cũ chỉ có tổng amount | Thêm `measure_details jsonb` và view `control.v_file_reconciliation_minimal` | SQL `008` backfill **chỉ** các row có `RECONCILIATION` SUCCESS đủ fare/tip; row thiếu chứng cứ để NULL |
| `audit.trip_fingerprint_history`, registry/archive và historical warning | Giữ bảng cũ nguyên trạng, bỏ khỏi DAG/runtime DDS mới | Không xóa, không giả trạng thái archive; dung lượng bảng/index cũ chưa thu hồi |

`005_storage_optimization_v2.sql` chứa source file version và legacy history; `006_backfill_recovery_and_cleanup.sql` chứa recovery/parent view; `007_replacement_recovery.sql` chứa binding thay version. Function đích ở `sql/control/003_control_functions.sql`, dbt selectors/models/tests ở `dbt_project`. Trước cutover phải rà `pg_depend`/`pg_get_viewdef`, grants, Power BI và job ngoài repo đang đọc cột/bảng cũ. Consumer dùng alias chỉ được chuyển sang view tương thích sau khi đã kiểm kết quả; không sửa view để che một cột không có dữ liệu thay thế.

## 3. Thứ tự migration và cutover

1. Dừng writer/refresh, ghi batch đang RUNNING/STALE và snapshot watermark. Hoàn tất hoặc phục hồi batch dở theo control; không sửa status trực tiếp.
2. Trên QA từ schema sạch: áp `001` control, `005` nếu base thiếu version metadata, `003` function đích, `006`, `007`, rồi [`008_minimal_dds_metadata.sql`](../../sql/migrations/008_minimal_dds_metadata.sql). Bản QA T09 dùng base control đã bao gồm phần `005`; `008` đã được áp dụng, rollback view và áp dụng lại.
3. Với database đang có **Fact legacy chứa raw/candidate fingerprint**, `008` chủ động từ chối. Dựng target DDS tối giản **song song** bằng schema/profile riêng; nạp lại từ Parquet bất biến đã xác minh theo version và đối soát từng file. Cần chốt tên target, quyền/grants và danh sách consumer thực tế trước khi viết cutover SQL riêng. Không `ALTER DROP COLUMN` Fact live hoặc chạy dbt full refresh trên nó.
4. Kiểm PK/unique/FK/check, index và `ANALYZE` ở target; so raw = canonical + technical, canonical = Fact + DQ, fare/tip/total eligible count và sum; chạy batch tests và full audit trên danh sách file đích. Kiểm retry/replacement, checksum skip, watermark và parent DDS SUCCESS khi OLAP PENDING.
5. Sau khi cùng người phụ trách BI xác nhận mapping, chuyển consumer sang target/view đích theo cửa sổ riêng; mở lại writer rồi reporting/refresh theo gate tương ứng. Lưu old schema và metadata để quay lại.

## 4. Rollback

Trước khi chuyển consumer: dừng writer và quay runner/profile về schema cũ; QA additive rollback [`008_minimal_dds_metadata_rollback.sql`](../../sql/migrations/008_minimal_dds_metadata_rollback.sql) chỉ bỏ view, **giữ** cột `measure_details` cùng audit evidence. Sau cutover: dừng writer/BI refresh, chuyển consumer và profile về bản cũ đã snapshot; phục hồi bằng backup nếu có dữ liệu mới cần đồng bộ. Không tự giảm watermark, xóa file version, Fact, DQ hoặc lịch sử. Side-by-side target chỉ được dọn bằng quyết định decommission riêng sau khi retention/backup được xác nhận.

## 5. Khảo sát database hiện có — 2026-11-24

Snapshot chỉ đọc tại `data/work/t09_postgis_qa/live_inventory.json`, từ container `nyc_taxi_bi-postgres-1`, database `nyc_taxi`, PostgreSQL 16.9. Database 24,046,850,531 byte (~22.40 GiB); 32 bảng được khảo sát, 687 column definitions, 45 indexes, 100 constraints, 6 views và 8 quan hệ dependency giữa views/tables. Không có `dds.fact_taxi_trip` hoặc bảng OLAP. Các bảng chiếm chỗ chính:

| Bảng | Dung lượng byte |
| --- | ---: |
| `dds.int_taxi_trip_fingerprinted` | 8,768,749,568 |
| `dds.int_taxi_trip_classified` | 8,655,347,712 |
| `stage.yellow_taxi_raw` | 2,467,782,656 |
| `stage.yellow_taxi_canonical` | 2,258,395,136 |
| `audit.trip_fingerprint_history` | 1,822,900,224 |

Batch 6 đang `STAGED`; các batch 2–5 FAILED. Watermark master version 0, chưa có successful batch, LSET 1970-01-01. Các trạng thái này được giữ nguyên. Chưa đủ điều kiện cutover tại chỗ: runner cũ/batch dở phải được owner xử lý bằng luồng recovery của đúng phiên bản, không đổi status bằng SQL để bỏ qua gate.

Phương án cho hiện trạng này: tạo **database đích riêng** (cùng các schema contract), áp DDL/migration đích, nạp lại từ immutable Parquet qua runner mới, giữ nguyên database cũ để rollback. Vì Fact cũ chưa tồn tại, không có thao tác copy/ALTER Fact cũ. Chỉ chuyển profile/kết nối consumer sau full audit và chốt batch dở. Sáu view hiện hữu cần giữ với database cũ; `int_taxi_trip_accepted` đang phụ thuộc classification cũ, nên không thay classification trong database cũ bằng model tối giản. Consumer ngoài DB/repo và kết nối Power BI phải được owner xác nhận trước cửa sổ cutover; inventory DB không thể tự tìm mọi client ngoài hệ thống.

Tại thời điểm khảo sát, ổ D còn khoảng 51.24 GiB, runner dự trữ tối thiểu 25% ổ (~36.62 GiB), còn khoảng 14.62 GiB cho peak tăng thêm. Một bản backup chưa nén theo upper estimate kích thước DB (~22.40 GiB) đã vượt phần tăng thêm này nếu đặt cùng ổ; chọn ổ khác cho backup trước triển khai live. Chưa cấp disk budget cho target full-month: cần đo bytes/row, workspace và WAL trong T10 rồi đưa `expected-peak-bytes` có căn cứ. Không dùng 50 MB forecast của QA cho live. Đây là gate triển khai live, không ngăn kiểm chứng fixture T09.

SQL `008` là migration metadata có thể áp dụng trên schema tương thích, không phải phép chuyển đổi Fact legacy sang Fact đích. T09 chỉ khảo sát và lập phương án; chưa áp migration, xử lý batch dở hoặc chuyển consumer của database hiện có.
