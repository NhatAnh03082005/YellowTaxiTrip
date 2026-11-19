# Fixtures cho contract dbt tối giản — T02

Đây là specification test data độc lập, không phải dữ liệu benchmark. Không nạp vào production. Không có SQL runtime mới được triển khai trong T02.

## Nội dung

- `input.json`: 38 raw records trong 6 processing scopes; raw values là text/null để giữ representation trước canonical rounding. File/schema SHA là synthetic identities, không phải checksum của JSON hay Parquet thật.
- `../../expected_results/dbt_minimal_rows.json`: expected literals từng case: disposition, toàn bộ rules mong đợi, 5 eligibility flags, source key và in-file candidate flag.
- `../../expected_results/dbt_minimal_summaries.json`: expected counts, fare/tip/total eligible count + sum từng nhánh, revenue và KPI numerator/denominator.
- `scenarios.json`: 12 acceptance scenarios cho SQL/runner/recovery về sau. T02 chỉ chuẩn bị scenario; không tuyên bố đã chạy 12 scenario trên PostgreSQL.
- `../../dbt_minimal_oracle.py`: oracle thuần Python Decimal/datetime/hashlib, không import SQL/Mage production; không đưa vào pipeline runtime.
- `../../test_dbt_minimal_contract.py`: 11 tests kiểm oracle với literals, golden hashes có sẵn, arithmetic/reconciliation và negative mutations. `compare_actual_rows` tái sử dụng được cho adapter SQL về sau.

## Chạy offline từ project root

```powershell
.\.venv-dbt\Scripts\python.exe -m unittest discover -s tests -p test_dbt_minimal_contract.py -v
```

Không cần Docker, database hay network. PASS chỉ xác nhận specification/fixture nhất quán. T03–T09 còn phải chạy output models/runner thật và so sánh; không gọi oracle thay SQL rồi ghi SQL PASS.

## Kết quả đã chốt

| Scope | Raw | Canonical | Fact | Technical | Business | Duplicate | Revenue |
|---|---:|---:|---:|---:|---:|---:|---:|
| A_2024 | 29 | 24 | 17 | 5 | 6 | 1 | 222.03 |
| B_2025 | 6 | 6 | 5 | 0 | 1 | 0 | 85.00 |
| C_CROSS_FILE | 1 | 1 | 1 | 0 | 0 | 0 | 13.00 |
| A_REPLACEMENT | 1 | 1 | 1 | 0 | 0 | 0 | 13.00 |
| EMPTY_VALIDATED_SCOPE | 0 | 0 | 0 | 0 | 0 | 0 | 0.00 |
| ALL_REJECTED | 1 | 0 | 0 | 1 | 0 | 0 | 0.00 |

Arithmetical review A: 29 raw − 5 technical = 24 canonical = 17 Fact + 6 business + 1 duplicate. Raw fare=29×10−11=279 (negative-fare case changes 10→−1); raw tip=29. Total has 27 numeric values: 27×13−13−11+0.03+1=328.03. Branch total 39 technical +13 duplicate +54 business +222.03 trusted. Revenue=17×13+0.03+1=222.03. Tip eligible excludes two non-card trusted records: 15/150. Duration excludes 4h warning: 16×600=9600. Speed additionally excludes 60 mph warning and zero-distance warning: 14×2/8400 seconds. Passenger sum=15+3+4=22 after excluding 0/null and expanding passenger 4/5 groups.

B: totals 15.75+13+15.75+15.75+28.75+11.75=100.75; CBD-before-effective reject removes 15.75 → revenue=85. Tip numerator=9, denominator=55; speed distance=17 miles, seconds=5520. These unequal fares/durations deliberately distinguish ratio-of-sums from mean-of-row-ratios.

After A+B+C: 23 Fact, revenue 320.03. Pickup 2024-05-01 includes A+B02+C: 19 trips, 248.03. Replace only A with R: 7 Fact, revenue 111; old shared day remains 2 trips/26 from B02+C; new day 2024-05-02 has 1/13. The source-month 2024-05 route 132→138 disappears; daily route on 2024-05-01 does not disappear because other files still contribute.

## Coverage và giới hạn

- A01/A02 exact duplicate: lowest row wins; A03 same candidate but component khác, A04 precision raw distance khác nhưng canonical cùng 2.000. Không được quarantine A03/A04 chỉ do canonical/hash candidate trùng.
- C01 same raw/hash/schema as A01 nhưng khác file checksum; chỉ TIME-005 vì source month=June. Không thêm DUP-002 hoặc sửa A01 hồi tố.
- B01/B03/B04: CBD present/valid, trước hiệu lực và expected-null component; A không có CBD field. Không suy schema chỉ từ năm trong adapter thật.
- Technical null/unparseable required fields theo Mage hiện tại; không dùng null required values để giả canonical warning. A12 dùng zone 999 có kiểu hợp lệ để kiểm LOC-001.
- Empty scope chỉ là downstream fixture để kiểm scoped zero-row deletion. `inspect_parquet` hiện từ chối file 0 rows; không thay rule ingestion.
- Oracle không giả lập đầy đủ PostgreSQL casts overflow/float/date formatting hoặc control transaction. Dùng fixtures cũ cho 5.000-row Parquet, technical CSV và SQL golden vector để mở rộng coverage. T04/T06 còn phải bổ sung boundary calendar/dimension/constraint tests đúng implementation.

## Adapter cho agent Cụm 2/3

1. Tạo QA schema/database cô lập; chỉ row fixture nhỏ. Không dùng helper SQL cũ mặc định trỏ live database.
2. Tạo fixture lookup đủ zones trong `known_zones`; dùng seed key mapping thật, geometry giữ reference riêng. Không yêu cầu 999 là member.
3. Mỗi scope chạy riêng; thêm các trường lineage từ file metadata; source_record_key theo contract. Giữ mapping `(file_version_id, source_row_number) → case_id` bên ngoài Fact để không đưa test labels vào schema runtime.
4. Raw lexical payload trong JSON là biểu diễn hợp đồng cho fixtures SQL, như SQL golden test cũ; nếu đi qua Parquet/Mage, cần adapter explicit tái tạo schema typed và kiểm fingerprint parity, không mặc định text fixture là Parquet production.
5. So classification và technical output hợp nhất theo case_id, rules sort, boolean chuẩn JSON; dùng `compare_actual_rows`. Giữ đủ rejected rows và source keys. So summary bằng expected JSON, không sinh lại expected từ models mới.
6. So hash vectors legacy v001/v002, survivor, nguyên tử tiền và eligible sets; không chỉ so count tổng.
7. T05/T08/T09 chạy scenarios.json với runner/control thực, log mapping trip_key trước/sau retry, old/new grains và watermark; đánh dấu từng case thực sự đã chạy.
8. Các differences được phép: bỏ historical DUP-002/archive/registry, normalize physical columns; không bỏ raw-exact policy. Thay expected ngoài allowlist phải sửa contract có giải thích, không sửa để che bug.

Không có generator tự ghi đè expected. Expected literals đã review bằng số học ở trên và golden vectors legacy; oracle đọc và so, không sản xuất golden results.
