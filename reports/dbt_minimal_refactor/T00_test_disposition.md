# T00 — Hướng xử lý test inventory (chưa sửa tests legacy)

Đây là bản đồ handoff; T06 thực hiện sửa/selector, không xóa tests chỉ theo tên. Generic YAML tests cần giữ unique/not-null/FK/required measures theo schema mới; bỏ not_null persistent business hashes sau khi T05 bỏ columns. Existing 170 manifest tests bao gồm generic tests, không chỉ 29 singular files.

| Singular test | Hướng xử lý |
|---|---|
| `business_code_members.sql` | Reference/init/full audit; không quét lại reference mỗi file nếu không đổi |
| `business_fixture_rules.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `cbd_schema_consistency.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `component_warning_policy.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `cross_file_match_retention.sql` | Loại khỏi pipeline mới vì feature lịch sử đã bỏ; giữ legacy evidence |
| `dimension_technical_members.sql` | Reference/init/full audit; không quét lại reference mỗi file nếu không đổi |
| `dq_policy_configuration.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `dq_rule_mapping_complete.sql` | Sửa: bỏ historical DUP-002, giữ in-file candidate diagnostic contract |
| `dq_rule_policy_consistency.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `fact_accepted_membership.sql` | Sửa: bỏ so physical hashes/lineage text, scoped accepted membership |
| `fact_analysis_bands.sql` | Sửa: chuyển kiểm derived analysis view thay physical Fact columns |
| `fact_business_code_members.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `fact_date_time_role_keys.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `fact_dimension_key_mapping.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `fact_identity_constraints.sql` | Sửa: giữ physical PK/unique và ổn định key khi retry, bỏ archive assumptions |
| `fact_kpi_eligibility.sql` | Sửa: giữ predicates, scope và normalized columns |
| `fact_lifecycle_history.sql` | Loại khỏi pipeline mới vì feature lịch sử đã bỏ; giữ legacy evidence |
| `fact_unknown_member_usage.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `file_month_and_cbd_consistency.sql` | Sửa: metadata join/source month khác analysis month |
| `file_reconciliation_balances.sql` | Sửa: thêm fare/tip eligible counts/sums, keep total và row balances |
| `fingerprint_history_candidate_parity.sql` | Loại khỏi pipeline mới vì feature lịch sử đã bỏ; giữ legacy evidence |
| `fingerprint_serialization_vectors.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `raw_fingerprint_input_contract.sql` | Sửa: kiểm raw đầu vào trong query/adapter, không đòi JSON persisted |
| `reference_seed_mapping_contract.sql` | Reference/init/full audit; không quét lại reference mỗi file nếu không đổi |
| `same_file_duplicate_survivor.sql` | Sửa: so current scoped classification/source lineage, không đòi persisted hash |
| `source_record_identity.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `trip_classification_partition.sql` | Giữ ý nghĩa; scope batch hoặc gộp scan khi không mất diagnostics; full audit riêng |
| `zone_geometry_membership.sql` | Reference/init/full audit; không quét lại reference mỗi file nếu không đổi |
| `zone_lookup_count.sql` | Reference/init/full audit; không quét lại reference mỗi file nếu không đổi |

## Python/QA integration tests cần cập nhật về sau

- test_fingerprint_golden_versions giữ raw golden vectors; candidate hash golden giữ vì vẫn transient candidate. Consumer-switch/candidate-history parity tests chỉ còn legacy, không thuộc minimal runtime.
- test_fact_lifecycle_scope, run_fact_replacement_qa, run_replacement_recovery_qa: thay archive-event assertions bằng scoped mutation, keys, status/watermark, affected OLAP correctness; không đổi input để che regression.
- test_duplicate_survivor_fixture_sql hiện cắt CTE theo marker current_cross_file_raw; T03 bỏ CTE đó nên phải sửa harness cùng change, giữ expected survivor.
- test_olap_kpi_fixture_sql hiện mong daily chỉ chứa current file; T07 thay expectation theo toàn affected day và thêm disappeared groups.
- test_backfill_safety_gates, recovery/control/cleanup tests: giữ failure/watermark/locks semantics, bỏ yêu cầu hardcoded archive ở runner mới. Không thay đổi chúng trong Cụm 1.
- test_dbt_minimal_contract mới là offline specification, không thay các SQL integration tests.
