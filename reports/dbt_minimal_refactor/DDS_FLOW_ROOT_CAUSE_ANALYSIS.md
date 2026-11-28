# Phân tích nguyên nhân hiệu năng toàn luồng Source → DDS

Ngày phân tích: **28/11/2026**. Branch `feature/optimize-mage-dds-load-performance`, HEAD `2c2b531`; giữ nguyên working tree có trước. Đây là chẩn đoán tiếp sau cặp đo May→June ngày27/11, không phải thay đổi yêu cầu hoặc nghiệm thu mới T10/T11.

## 1. Kết luận chính

Chi phí lớn đến từ **serialize/hash raw, sort/window trên nhiều triệu dòng, materialization và đọc lại dữ liệu trong tests, kiểm identity lặp và ghi/index/WAL của Fact hiện hữu**. Trạng thái cache và hoạt động nền của PostgreSQL làm thời gian từng lượt dao động. Không đủ bằng chứng quy toàn bộ việc tháng6 chậm hơn cho lượng Fact lịch sử hoặc setting32MB.

Phần đã xác nhận:

| Nguyên nhân | Bằng chứng thực tế | Ý nghĩa |
| --- | --- | --- |
| Classification có hai sort rộng | Hai external merge sort dùng1,269/1,553GiB disk; hash join32batches | Tăng memory32MB chưa giữ được các tập dữ liệu này trong RAM; còn nhiều CPU và I/O tạm |
| Policy lặp theo từng chuyến |3.539.193lượt aggregate, chỉ182tổ hợp rule; policy26rows bị scan1.227.999lần | Có cơ hội tái sử dụng policy theo tổ hợp rule động |
| Fact test materialize rộng | `current_fact` spool khoảng1,011GiB rồi được scan4lần | Một test có thể đọc/ghi nhiều lần lượng dữ liệu của file |
| Partition test ước lượng sai NDV | Ước lượng200groups, thực tế3.539.193; HashAgg101batches | Khối tổng hợp lớn phải chia batch và ghi hơn6GiB temp trong phép thử |
| Scope/identity lặp |4scope gates trong build June, log ghi tổng76.809s; raw/canonical identity probes6.969/6.578s | Kiểm lại source identity là chi phí đáng kể, nhưng không được bỏ gate mà thiếu proof |
| Các writer tranh cùng khóa | Riêng lệnh lấy lock có lượt40.997s | Node duration chứa thời gian chờ, không phải toàn bộ thời gian tính/ghi của node |
| Fact hiện hữu khác initial load | Nguồn tạm33.703s, insert/upsert74.525s, cùng scope/lock/constraints | Đây là khác biệt thực giữa tháng đầu và tháng tiếp theo |
| WAL/checkpoint pressure | Default max_wal_size1GB; nhiều cảnh báo11–23s/checkpoint | Có yếu tố hạ tầng ảnh hưởng I/O và độ ổn định số đo |
| Shared memory container nhỏ | `/dev/shm=64MiB`; probe inline lỗi khi xin thêm16MiB | Biến thể tăng parallel hash có thể fail dù C còn trống |

## 2. Phương pháp và giới hạn

- Đọc log SQL từng statement và artifacts của cả baseline/candidate, hai tháng; đối chiếu code/materialization/test contract.
- Tạo bản sao QA mới `dbt_diag_flow_20261127` từ QA candidate đã PASS. Phục hồi đúng3.539.193dòng June vào physical slim-v2 bằng các hàm chuyển đổi/streaming hiện có của Mage, kiểm SHA256 và raw/canonical counts. Không chạy lại ingestion hoặc đổi trạng thái trên main.
- Control SUCCESS của bản sao được giữ nguyên. Không gọi processing materialization để giả writable. Classification được thực thi từ compiled SELECT vào bảng riêng `dds.diag_classified`; selected test SQL đọc bảng này. Không chạy full scope gate trên snapshot SUCCESS rồi coi nó là lượt pipeline mới.
- Lấy25plan artifacts gồm estimates và `EXPLAIN ANALYZE/BUFFERS/SETTINGS`, bật `track_io_timing` **trong transaction chẩn đoán**. Các query thử nghiệm thay đổi planner settings chỉ trong session QA. Có một probe inline thất bại do shared memory, được giữ nguyên log và ghi rõ bên dưới.
- Giới hạn QA: C floor25GiB, cap tăng16GiB mỗi action, statement15phút/temp_file_limit8GiB cho các probe nặng; các probe scope/policy timeout120s. Không đổi settings toàn server, durability, threads hoặc container.
- Số đo chẩn đoán đã có cache/stats/trạng thái server khác thời điểm chạy pipeline. Instrumentation và phạm vi SELECT/CTAS/hook cũng khác. **Không thay timer trọn luồng1571.500s bằng tổng các probe hoặc dùng chúng để báo PASS SLA.** Node times/blocks của plan là inclusive; không cộng toàn cây vì sẽ đếm lặp.

## 3. Vì sao không thể giải thích mọi khác biệt bằng “Fact lớn hơn”

Compiled classification của baseline May/June và candidate May/June có cùng SQL sau chuẩn hóa database, batch/version và source SHA:16.523ký tự, SHA256 `66a9d83bc70235c875654ed1cbc05e09ae41aea8528d61c904fd6858c5c83a6b`. SQL không đọc `fact_taxi_trip` hoặc lịch sử fingerprint. Test duplicate cũng chỉ đọc classification, duplicate output và candidate summary của scope.

Vì vậy Fact có lịch sử giải thích phần đường ghi Fact, nhưng không trực tiếp giải thích classification280.62s hoặc duplicate test91.26s của June. Chưa có so sánh đủ phân phối dữ liệu/rule của May để khẳng định June “bẩn hơn” là nguyên nhân chính.

Trên candidate, June còn ít raw hơn May:3.539.193so3.723.833. Classification262.08→280.62s và tests171.469→196.531s cần được xem cùng query shape, cache, spill, lịch chạy và I/O nền; không chỉ so row counts.

## 4. Classification: chi phí nằm ở đâu

Log production-path candidate June: node280.62s, trong đó CTAS240.468s; phần còn lại gồm input statistics, scope validation, constraints, index và ANALYZE. CTAS là ưu tiên lớn nhất.

Plan thực trên full June QA:

- CTAS có execution173.690s trong môi trường chẩn đoán; không phải số đo cải thiện so240.468s vì điều kiện khác.
- Raw conversion/projection/serialization/hash branch có inclusive time khoảng56.151s. Không gọi đó là56s chỉ chạy SHA256: thời gian bao gồm chuẩn bị JSON/text và đọc nguồn.
- Raw hash được MATERIALIZED rồi join về canonical. Hash join có32batches; đây là spill thực, không chỉ dự đoán từ cost.
- Hai sort theo exact hash/row ordinal và candidate hash có Plan Width829/879bytes (**ước lượng**), disk sort size1.269/1.553GiB (**thực đo**). Các window mang theo nhiều trường của chuyến dù key xếp hạng chỉ gồm hash/order.
- Toàn root plan ghi4.613GiB temp và đọc4.148GiB temp. Temp I/O timing khoảng19.294s; vẫn còn nhiều chi phí xử lý/projection/so sánh/ghi đích. Không đủ cơ sở nói mọi chậm trễ là do ổ đĩa hoặc thiếu RAM.
- JIT tổng1.923s. Plan classification không có Gather/parallel workers; tăng dbt threads không tự chia truy vấn classification này ra nhiều CPU.

### Policy lookup có thể tái sử dụng

LATERAL policy aggregate thực hiện3.539.193lần. Dữ liệu có2.311.194rows không phát sinh rule; phần còn lại làm bảng policy26rows bị scan1.227.999lần. Toàn scope chỉ có182mảng `dq_rule_ids` khác nhau.

Thử riêng **cùng SQL policy**, cùng `diag_classified`,32MB và transaction read-only, chỉ bật/tắt session Memoize:

| Phép thử thành phần | Wall | Số lần aggregate policy |
| --- | ---: | ---: |
| Memoize off |14.000s |3.539.193 |
| Memoize on |2.203s |182 |

Cache có3.539.011hits/182misses, không eviction/overflow, peak31KiB. Các tổng kiểm tra count/cờ policy/severity của hai query giống nhau. Đây là bằng chứng cơ hội reuse theo **rule array động**; không hardcode182tổ hợp vào source. Query gốc đang không chọn Memoize ở vị trí này. Cần thay query shape rồi kiểm strict output/full-flow trước khi đưa vào runtime; không lấy14→2s làm tỷ lệ cải thiện toàn classification.

## 5. Tests: nhiều vòng quét và spill, không chỉ số lượng tests

### Fact test

`current_fact` có4tham chiếu nên bị materialize. Plan đang dùng index theo `file_version_id`, đọc đúng3.474.751Fact June; không phải quét toàn bộ lịch sử vì thiếu version index. Tuy nhiên spool của CTE rộng ghi132.477blocks, khoảng1.011GiB, rồi được đọc lại ở bốn nhánh values/policy/FK/mapping.

Probe32MB ghi tổng2.359GiB/đọc4.380GiB temp; hash của accepted có16batches. Cùng dữ liệu tốt, query trả0failure rows. Đây là chi phí kiểm tra và tổ chức SQL, không phải phát hiện sai dữ liệu.

### Duplicate test

Test tính lại window để kiểm độc lập survivor/occurrence/candidate, sau đó đọc `ranked` qua nhiều nhánh. Có2parallel workers thực, nhiều external sorts và materialized CTE rộng. Memory32MB giảm temp writes7.336→3.919GiB so4MB trong probe, nhưng bare-query wall32.141→31.360s gần nhau. Việc giảm spill chưa bảo đảm giảm wall tương ứng.

### Partition test

Các nhánh raw/canonical/technical/classified/Fact/business/duplicate được aggregate theo key, UNION ALL, rồi aggregate lại. Final per-key HashAgg dự đoán200groups trong khi thực tế3.539.193: lệch khoảng17.696lần. Với memory hiện tại nó phải chia101batches, disk usage riêng khoảng2.017GiB. Toàn query ghi6.134GiB/đọc4.951GiB temp, wall39.656s khi chạy riêng. Sửa ước lượng có thể cải thiện lựa chọn plan và các vòng spill, nhưng chưa chứng minh chỉ cập nhật statistics là đủ cho tập groups lớn này.

Biến thể “mỗi row đóng góp1, aggregate một lần” từng bị loại vì không chứng minh lợi ích toàn pha ở lượt trước. Không tự áp lại chỉ từ cost mới; candidate tiếp theo phải chứng minh plan/cardinality và correctness tốt hơn, rồi đo toàn luồng.

### Vì sao tests từng lên101/91s?

Đã lấy **đúng câu SQL có wrapper `count(*)/should_warn/should_error` từ log dbt**, không chỉ dùng compiled body:

| SQL full June trên cùng QA | Chạy riêng | Chạy đồng thời hai query |
| --- | ---: | ---: |
| Fact test wrapper |22.188s |26.906s |
| Duplicate test wrapper |24.656s |28.546s |

Hai query đồng thời hoàn tất28.609s; chạy riêng cộng46.844s. Concurrency làm mỗi node chậm hơn nhưng vẫn có thể giảm tổng wall. **Không được tự đổi về1thread chỉ vì một node chậm.** Cặp đo này không tái hiện mức100.839/91.175s SQL trong log pipeline June. Lượt cũ không lưu execution plans, buffer/I/O timing hoặc CPU/wait samples nên chưa thể phân bổ chính xác phần tăng vào cache/checkpoint/concurrency. Không chứng minh32MB là thủ phạm, cũng chưa chứng minh đã hết dao động.

### Probe inline: có lợi ích nhưng có lỗi tài nguyên

Probe `current_fact AS NOT MATERIALIZED` làm planner chọn Parallel Hash. Trên container `/dev/shm=64MiB`, query lỗi `could not resize shared memory segment ... to 16777216 bytes: No space left on device`. Lúc đó C vẫn có hàng chục GiB trống; sau query rollback, `/dev/shm` trở lại còn khoảng63MiB. Đây là budget shared-memory riêng, không phải disk guard C bị chạm.

Probe khác chỉ trên QA, thêm `SET LOCAL enable_parallel_hash=off`, hoàn tất13.219s và0failure rows; temp writes2.673GiB, cao hơn control2.359GiB. Nó có thêm parallel scans và điều kiện khác control. **Chưa có candidate production được chấp nhận:** phải xử lý resource budget, negative fixtures và đo toàn phase/flow, không thay trực tiếp CTE rồi coi là sửa xong.

## 6. Writer lock và scope validation

Các materialization cùng lấy `yellow_taxi_staging_writer`. Quarantine có lượt riêng lệnh lấy lock40.997s; con số đó chủ yếu là chờ writer khác. Không cộng các lock waits chồng nhau vào tổng luồng hoặc coi toàn bộ chúng là thời gian có thể tiết kiệm.

June candidate có scope-validation statements: classification16.182s, business26.796s, duplicate16.873s, Fact16.958s; tổng log76.809s. Mỗi gate kiểm lại raw/canonical identity, gồm tính source key từ SHA+row ordinal. Hai SELECT EXISTS identity tách riêng trên full June QA mất6.969s và6.578s.

Slim-v2 raw/canonical là hai view của cùng physical data; đây là hướng kiểm giảm scan lặp có căn cứ. Tuy nhiên chỉ được gộp sau khi chứng minh tương đương identity/lineage và bảo vệ scope khi retry/concurrency. Không bỏ gate bằng giả định “Mage đã kiểm rồi”, không dùng marker cũ cho workspace đã bị sửa.

## 7. Fact, index và checkpoint

### Đường initial và đường có lịch sử khác nhau

Initial target dùng TEMP VIEW, insert mới rồi bulk-build PK/unique/version index. Existing target giữ constraints/index, dựng nguồn tạm, upsert bảo toàn key và kiểm stale/replacement. Với June candidate, các statement chính trong node Fact151.75s:

| Bước | Giây |
| --- | ---: |
| Lấy writer lock |16.770 |
| Scope validation |16.958 |
| Dựng nguồn tạm có thứ tự |33.703 |
| Unique index nguồn tạm |2.611 |
| Insert/upsert |74.525 |
| ANALYZE target |2.536 |

So baseline June, insert121.431→74.525s, temp index15.872→2.611s; collision pre-join32.718s được thay bằng guard trong conflict lookup. Những lợi ích này đã nằm trong full-flow proof trước; không tính thêm lần nữa vào7,63%.

### Tăng size nằm ở index

Heap Fact gần như bằng nhau:2.424.119.296→2.424.102.912bytes. PK và version index bằng nhau. Unique source-key index762.478.592→859.512.832bytes, tăng97.034.240bytes, giải thích gần hết mức tăng database96.976.896bytes. Chưa đo leaf density/page splits để gọi đó là dead-tuple bloat hoặc chứng minh cơ chế split cụ thể. Không tự REINDEX hoặc bỏ unique index trong pipeline.

### Cấu hình WAL và hoạt động nền

Server hiện có shared_buffers128MiB, wal_buffers4MiB, max_wal_size1GiB, checkpoint_timeout300s, completion_target0.9; Docker VM thấy khoảng7,61GiB RAM. shared_buffers không phải toàn bộ RAM PostgreSQL/OS có thể dùng. Các setting không được đổi trong chẩn đoán.

Log server trong các cửa sổ dựng DDS:

| Lượt | WAL-triggered checkpoint starts | Cảnh báo quá thường xuyên |
| --- | ---: | ---: |
| Baseline May |6 |5 |
| Baseline June |22 |20 |
| Candidate May |7 |5 |
| Candidate June |9 |7 |

Có cảnh báo checkpoint cách nhau11–23s, rõ ràng ngắn hơn timeout5phút vì WAL threshold. Các checkpoint là hoạt động **cấp cluster**, không phải per-query timers. Không lấy duration checkpoint làm thời gian query bị đứng, không suy WAL bytes từng statement từ số checkpoint. Tuy nhiên đây là bằng chứng áp lực ghi nền và điều kiện cache/I/O khác nhau giữa các phase. Muốn định lượng phần đóng góp cần A/B riêng trên PostgreSQL QA tách biệt với budget WAL rõ; không tự tăng setting toàn server chính hoặc tắt durability.

## 8. Thứ tự xử lý tiếp theo

1. **Policy query shape:** dùng key rule array rõ để tái sử dụng policy động; tránh aggregate/scan policy theo từng row. Giữ precedence, unmapped handling và policy configurable; chạy full-row parity trước/sau.
2. **Tests:** xử lý spool rộng của Fact, grouping/cardinality của partition và đọc lại ranked trong duplicate. Giữ đủ failure conditions và negative fixtures; kiểm budget parallel/shared memory trước full run.
3. **Scope validation:** đo và gộp công việc identity trùng của slim-v2 khi chứng minh đúng contract, thay vì bỏ validation. Giữ wide/slim-v1 và tamper/retry cases.
4. **Classification:** giảm tuple mang qua sort/window, tránh conversion lặp theo exact serializer hiện hữu; hash/precision/missing-key/schema v001/v002 phải khớp. Không dùng biến thể hẹp cũ từng thất bại mà thiếu benchmark mới.
5. **Đo lại toàn chuỗi:** cùng source/profile, theo dõi wait events/I/O/WAL/CPU, lưu actual plans; kiểm cache và workloads khác, đo lặp phù hợp. Chỉ giữ tối ưu nếu full flow và correctness cùng tốt. Budget WAL/shm thuộc hạ tầng phải thử riêng, có rollback/budget; không đổi main để làm số đo đẹp.

Không hạ mục tiêu360s DDS/600s Source→DDS, không bỏ tests/quarantine, không lọc bớt thời gian/dữ liệu hoặc triển khai OLAP/T12. Cặp full-flow1701.297→1571.500s vẫn là **lợi ích quan sát của lượt trước**, chưa phải SLA ổn định hoặc proof24file. T10/T11 giữ IN_PROGRESS.

## 9. Trạng thái sau chẩn đoán và evidence

Main `nyc_taxi` kiểm **read-only**: batch1/2SUCCESS, watermark2/last batch2, Fact7.135.797, business127.229, total/revenue206.940.635,71; staging/mapping/classification0. Server track_io_timing vẫnoff, work_mem4MB, max_wal_size1GB; không còn active client job. Không sửa runtime source, metadata Mage, database chính, container hoặc dữ liệu nguồn trong lượt phân tích này.

Evidence local ignored tại `data/work/dds_flow_tuning_20261127/`:

- `statement-diagnosis.json`: SQL/timing từng statement của hai variant/hai scope.
- `diagnosis/{classification32.json,classification-sql-comparison.json,classification-counts.json}`.
- `diagnosis/*-estimate.json`, `*-actual.json`, `*-wrapped.json`, `concurrent-wrapped-pair.json`.
- `diagnosis/policy-memoize-{off,on}.json`, `rule-patterns.json`, `*-identity-gate.json`.
- `diagnosis/{storage-statistics.json,postgres-checkpoints.log,checkpoint-counts.json,wal-settings.json,measurement-summary.json,main-readonly-final.json}`.
- `diagnosis-probe.log`: lỗi shared memory của candidate inline ban đầu; giữ nguyên, không ghi đè bằng lượt thành công khác.

Không commit harness, payload, profiles hoặc artifacts QA. [T11](T11.md) giữ báo cáo hiệu năng toàn chuỗi và strict output parity; báo cáo này giải thích nguyên nhân, các thử nghiệm và phần chưa kết luận được.

Tài liệu kỹ thuật đối chiếu: [PostgreSQL16 EXPLAIN](https://www.postgresql.org/docs/16/using-explain.html), [CTE materialization](https://www.postgresql.org/docs/16/queries-with.html#QUERIES-WITH-CTE-MATERIALIZATION), [Resource consumption](https://www.postgresql.org/docs/16/runtime-config-resource.html), [WAL/checkpoints](https://www.postgresql.org/docs/16/wal-configuration.html). Các nguồn này giải thích cơ chế; các số đo ở trên đến từ artifacts cục bộ.

## 10. Theo dõi triển khai sau phân tích

Đã triển khai raw scalar, policy theo pattern động, Fact test scan và partition aggregation. Regression PASS; ba thay đổi đầu đo trọn hai file869.063s, strict output parity PASS nhưng June DDS368.531s còn vượt360s. Partition cuối PASS regression/probe, full-flow chưa xong trước yêu cầu xóa dữ liệu trên C. Đã xóa main/QA cùng volume project; source/logs trên D giữ nguyên. [T11](T11.md) ghi CODE_READY/IN_PROGRESS, số đo, trade-off và cleanup. Kết luận “không sửa runtime/database” ở mục9 áp dụng cho lượt chẩn đoán lịch sử, không áp dụng cho các lượt triển khai/cleanup sau đó.
