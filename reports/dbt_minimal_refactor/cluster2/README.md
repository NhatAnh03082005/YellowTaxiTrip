# Cụm 2 — evidence index

Lượt verification cuối: actual dbt/SQL QA PASS. results.json là danh sách invocation có thẩm quyền; log khác ngoài danh sách dưới có thể thuộc lần phát triển trước, giữ để điều tra. Không lấy run_results cuối làm kết quả cả suite.

- PostgreSQL 18 riêng, không Docker/production. Technical routing fixture supplied by T02 literals, không chạy Mage.
- 25 dbt calls: 13 thành công, 12 lỗi kỳ vọng; đã kiểm exit code, đúng error message và assertions dữ liệu/rollback liên quan.
- Sáu legacy tests PASS: [log](legacy_sql_and_unit_tests.log).
- Offline/fixtures/goldens/diff/docs: [verification](verification.txt).
- Physical schema: [snapshot](physical_schema.json). Footer 24 files: [calendar metadata](calendar_metadata.json).
- Full-month/peak disk chưa đo; OLAP/runner mới và PostgreSQL 16/PostGIS còn T06–T09.

| Invocation | Kết quả | Log |
|---|---|---|
| 00_parse_101 | SUCCESS | [log](00_parse_101.log) |
| 01_seed_101 | SUCCESS | [log](01_seed_101.log) |
| 02_run_101 | SUCCESS | [log](02_run_101.log) |
| 03_run_101 | SUCCESS | [log](03_run_101.log) |
| 04_run_101 | SUCCESS | [log](04_run_101.log) |
| 05_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](05_run_101.log) |
| 06_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](06_run_101.log) |
| 07_run_102 | SUCCESS | [log](07_run_102.log) |
| 08_run_103 | SUCCESS | [log](08_run_103.log) |
| 09_run_104 | EXPECTED_FAIL — đúng nguyên nhân | [log](09_run_104.log) |
| 10_run_104 | EXPECTED_FAIL — đúng nguyên nhân | [log](10_run_104.log) |
| 11_run_104 | SUCCESS | [log](11_run_104.log) |
| 12_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](12_run_101.log) |
| 13_run_101 | SUCCESS | [log](13_run_101.log) |
| 14_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](14_run_101.log) |
| 15_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](15_run_101.log) |
| 16_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](16_run_101.log) |
| 17_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](17_run_101.log) |
| 18_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](18_run_101.log) |
| 19_run_101 | EXPECTED_FAIL — đúng nguyên nhân | [log](19_run_101.log) |
| 20_run_106 | SUCCESS | [log](20_run_106.log) |
| 21_run_106 | SUCCESS | [log](21_run_106.log) |
| 22_run_105 | SUCCESS | [log](22_run_105.log) |
| 23_run_105 | EXPECTED_FAIL — đúng nguyên nhân | [log](23_run_105.log) |
| 24_parse_105 | SUCCESS | [log](24_parse_105.log) |
