# Cụm 2 — DDS tối giản T03–T05

Ngày: 2026-11-19. Đọc cùng `DBT_MINIMAL_CONTRACT.md` và reports T03/T04/T05. Đây là implementation DDS có opt-in; chưa là pipeline production hoàn chỉnh, chưa migrate database live.

## Bật đúng phạm vi

`dbt_minimal_mode` mặc định **false**, giữ luồng legacy cho lời gọi dbt thủ công. Runner T08 truyền `true` tường minh và cần database đã chuẩn bị đúng schema. Không trộn hai mode trong cùng target database đang chứa dữ liệu thật. T09 phải chuẩn bị migration/rollback trước khi chuyển target hiện hữu.

Các biến bắt buộc khi bật mode:

```yaml
dbt_minimal_mode: true
load_batch_id: <positive integer, đúng batch STAGED/DBT_RUNNING/RECONCILING>
load_file_version_id: <positive integer, đúng immutable version>
```

Không truyền `fact_lifecycle_mode: archive`, history candidate flags hoặc `--full-refresh` cho Fact/DQ. Archive/registry cũ có thể còn tồn tại; mode mới không đọc/ghi hoặc tự xóa chúng. Runner T08 từ chối các biến legacy này; chưa chạy trên live.

Chỉ chạy trên QA/target mới đã chuẩn bị đúng sources/seeds/reference. Ví dụ sau chỉ mô tả selection, không phải lệnh migrate live:

```powershell
# Phải chỉ định --profiles-dir trỏ QA đã kiểm tra; không dùng .dbt live.
dbt seed --project-dir dbt_project --profiles-dir <QA_PROFILE> --vars '<SCOPE_JSON>'
dbt run --project-dir dbt_project --profiles-dir <QA_PROFILE> --select +tag:minimal_dds --vars '<SCOPE_JSON>'
```

Seed khi thiếu bảng reference trong target; không seed lại từng file. Selector `minimal_dds_run` gọi dimensions, materialization cache tránh ghi lại reference không đổi. Khi thay đổi nội dung seed, cần chạy init reference có kiểm soát trước batch mới. Nạp staging/mapping/source registry do Mage/control sở hữu, không do ví dụ dbt trên tự tạo.

## T03: classification dùng chung

- Staging view lọc batch+version; preflight kiểm toàn staging raw/canonical không trộn scope, metadata/schema/checksum/source key khớp registry, đúng một STAGED attempt.
- Fingerprint SQL được `ephemeral` khi minimal; không có physical fingerprint table/view mới. Chỉ classification là table làm việc nặng, có unique source key, index version/disposition và ANALYZE.
- T11: kiểm sự có mặt của `raw_payload`/CBD khi compile để tránh chuyển cả hàng sang JSON chỉ để kiểm hai key; JSON đầu vào hash vẫn giữ nguyên, không đổi serializer hoặc tạo thêm workspace.
- Giữ raw-exact serialization 19/20 field và candidate 8 field, window survivor nhỏ nhất trong file; bỏ historical cross-file queries. Candidate chỉ diagnostic, không tự đổi disposition.
- Classification output không có JSON/raw payload, file text/checksum/schema lặp lại, bands hoặc toàn bộ rule boolean. Có transient hashes, survivor/occurrence, raw_contract_valid/has_unmapped_rule để enforce contract.
- Post-build CHECK chặn thiếu raw hash input/null hash hoặc DQ rule không có mapping. Summary lưu candidate rows/groups một dòng/batch/version.
- Workspace marker gồm batch/version/valid. Trước rebuild, hook ngoài transaction invalidate marker và commit; failed rebuild giữ bảng cũ để điều tra nhưng marker invalid nên Fact/DQ không dùng được. Sau build hợp lệ, table và marker valid commit cùng nhau. Invalidation chỉ đổi metadata nội bộ, không publish/commit Fact hay watermark.
- dbt table swap có thể bỏ các view phụ thuộc bảng cũ. Sau classification phải dựng lại `int_taxi_trip_accepted` và analysis view trong DAG đầy đủ; không chỉ chạy classification rồi gọi riêng Fact. Marker đúng không thay thế việc dựng dependency views. QA calendar negative test dựng accepted trước để xác nhận lỗi đúng là ngày ngoài calendar, thay vì một lỗi missing-view không liên quan.
- Vẫn yêu cầu một writer toàn pipeline; advisory lock ở các mutation chỉ hỗ trợ từng transaction, không thay global runner lock của T08.

## T04: references không dựng lại mỗi tháng

`minimal_reference` cache signature từ compiled SQL và nội dung dependency reference nhỏ. Không đọc accepted để tính date bounds.

- Calendar mặc định `2002-01-01`..`2026-12-31`, từ footer metadata 24 file: min 2002-12-31, max 2026-01-05. Footer bounds chứa cả raw anomaly, không phải accepted-row profiling. Chi tiết `reports/dbt_minimal_refactor/cluster2/calendar_metadata.json`.
- Calendar chỉ mở rộng/config-update có kiểm soát; không cho thu hẹp dates hiện có. Fact fail trước mutation nếu timestamp accepted không map date, không âm thầm gán key 0.
- Time giữ 1.440 business minute members + 0/-1. Zone/Vendor/Payment/Rate cập nhật Type 1 khi reference signature đổi; không ghi lại rows không đổi.
- Signature không đổi thì chỉ SELECT, không rebuild/upsert dimension data và không thay refreshed_at.
- Surrogate mapping drift cho cùng natural code fail, yêu cầu migration; không tự rekey Fact.
- Sửa staging zone đọc thêm physical `zone_name` của Mage ngoài `Zone`/`zone`, để không mất tên zone khi physical DDL dùng lowercase.
- Geometry vẫn là physical reference/source riêng, không nhân bản trên Fact; QA này không kiểm PostGIS polygons.

## T05: Fact/DQ gọn theo version

- Fact simple upsert giữ trip_key cho source key đang hiện diện; unchanged rows không UPDATE (QA đối chiếu xmin). Có PK identity, unique source key, required-key constraints và index file_version_id trên target mới.
- Anti-join xóa stale Fact trong current version, kể cả zero accepted output. Không upsert source-key collision sang version khác.
- DQ `minimal_file_replace` xóa/ghi đúng current version trong transaction của từng model; output rỗng vẫn clear stale rows. Quarantine không lưu raw JSON/business hashes, giữ canonical diagnostics/reasons; technical DQ Mage giữ nguyên.
- Replacement chỉ dùng đúng binding old/new đã được control chấp thuận. Checksum đổi không có binding fail preflight. Fact delete/upsert cùng transaction; DQ/Fact/OLAP không chung một transaction.
- Trước Fact mutation, ghi tập affected pickup dates + source months old/new vào `dds.dbt_minimal_affected_grains`. Giữ union qua retry, không reset mất old grains sau một model đã commit. Đây là metadata tổng hợp nhỏ, không lưu prior_fact hay event trên từng chuyến. T07 dùng để rebuild OLAP, T08 quy định retention/cleanup.
- Column names và physical types của target hiện hữu phải khớp model; nếu khác thì fail, không auto-drop/migrate. T09 phải thêm schema/constraint migration cho target legacy.
- `v_fact_taxi_trip_analysis` là view join file metadata và derive passenger/distance/time bands, row tip rate, lineage alias để không lưu text/diagnostics lặp trên mọi Fact row.

## Ranh giới Cụm 3 — DDS và reporting độc lập

- OLAP legacy disabled trong minimal mode cho đến T07: nó còn dùng physical source_year_month trên Fact và current-file-only daily refresh. Không chạy câu SQL đó trên slim Fact.
- Các singular tests đòi cột đã bỏ hoặc lifecycle/cross-file bị gate chỉ trong minimal mode. T06 thay bằng scoped batch tests/full audit phù hợp. Config/rule consistency/mapping, partition và core generic FK/identity tests phù hợp vẫn giữ.
- `file_reconciliation` legacy chưa bổ sung fare/tip/eligible sums mới; T06 làm. QA Cụm 2 dùng các assertions độc lập đầy đủ fare/tip/total counts/sums và KPI để kiểm DDS, không tuyên bố reconciliation runner đã hoàn chỉnh.
- T08 đã chuẩn bị runner skip/retry/cleanup và artifact riêng từng invocation. Database QA end-to-end, migration và lịch tự động cần T09 kiểm chứng. Không dùng `--resume-dbt-select` để bỏ upstream nếu không xác nhận workspace valid.
- Không cleanup staging/workspace, advance watermark, backfill 24 tháng hoặc xóa legacy trong Cụm 2.

## QA thực tế và tái chạy

Dùng PostgreSQL **18** cài sẵn, instance riêng dưới `tmp/dbt_minimal_cluster2_pg18`, bind `127.0.0.1:15438`, database `dbt_minimal_cluster2`, user `dbt_minimal_qa`. Không dùng `.env` hoặc profile production. Instance không thay service PostgreSQL/Docker sẵn có.

Harness `tests/run_dbt_minimal_cluster2_qa.py` chỉ chấp nhận đúng port/database/user trên; reset/truncate chỉ fixture output của database này. Profile QA ở `tmp/dbt_minimal_cluster2_profile`. Instance cần được start riêng trước khi chạy; sau kiểm chứng sẽ stop, giữ files để điều tra.

```powershell
.\.venv-dbt\Scripts\python.exe tests/run_dbt_minimal_cluster2_qa.py --port 15438 --database dbt_minimal_cluster2 --user dbt_minimal_qa
```

Actual SQL/DBT chạy fixture scope, so literal T02 dispositions/rules/eligibility, row counts và fare/tip/total eligible sums từng nhánh, KPI sufficient stats và FK roles. Test warm retry, reference-change/rekey/shrink guard, explicit replacement, rollback bằng QA trigger gây lỗi sau delete, missing raw/policy, mixed scope, wrong/invalid workspace, all-reject/empty output, legacy schema mismatch. Legacy raw-hash/survivor/OLAP vectors chạy trên QA qua temp tables + ROLLBACK; không gọi Docker/live.

Giới hạn: technical routing trong adapter dùng expected fixture để chuẩn bị output Mage, không chạy Mage; raw lexical payload fixture không thay typed Parquet/Mage integration. PostgreSQL 18 không thay verification production PostgreSQL 16/PostGIS ở T09. Không có full-month benchmark hoặc peak storage trong cụm này.
