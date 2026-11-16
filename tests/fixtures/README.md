# Test fixtures

Các fixture trong thư mục này đủ nhỏ để commit và cho phép Mage/dbt development không phụ thuộc đường dẫn hoặc 24 file Parquet lớn trên máy Project Lead.

## Representative source fixtures

| Fixture | Nguồn | Rows | Mục đích |
|---|---|---:|---|
| `valid/yellow_tripdata_2024-05_sample.parquet` | `yellow_tripdata_2024-05.parquet` | 5,000 | Schema 2024, chưa có CBD fee |
| `valid/yellow_tripdata_2025-05_sample.parquet` | `yellow_tripdata_2025-05.parquet` | 5,000 | Schema 2025, có CBD fee |

Tên thư mục `valid` có nghĩa file Parquet và schema nguồn hợp lệ. Mẫu cố ý giữ cả dòng bình thường, warning và business-quarantine candidate để kiểm tra DQ. Mỗi mẫu được chọn bằng seed cố định trên tất cả row group, không chỉ lấy các dòng đầu.

Coverage thực tế gồm passenger null/0/>6, Zone 264/265, amount hoặc distance không dương, business unknown/Flex Fare code và positive CBD fee ở mẫu 2025. Số lượng, source SHA-256, seed và fixture SHA-256 nằm trong `fixture_manifest.json`.

## Invalid fixtures

- `invalid/business_rule_cases.parquet`: chín dòng có kiểm soát cho TIME-002, DIST-001, PASS-001, AMT-001, CODE-003, LOC-001, SPEED-002 và DUP-001. Kết quả mong đợi nằm tại `tests/expected_results/business_rule_cases_expected.csv`.
- `invalid/technical_cast_errors.csv`: năm dòng chuỗi lỗi cho CAST-001, CAST-002 và CAST-003. CSV được dùng vì Parquet typed source không thể chứa chuỗi sai kiểu trong cột timestamp/numeric.

## Tạo lại fixture

Đảm bảo 24 file nguồn và catalog đã được validate, sau đó chạy từ project root:

```powershell
.\.venv\Scripts\python.exe .\profiling\scripts\build_test_fixtures.py
```

Seed và selection policy cố định. Chạy lại từ cùng source SHA-256 phải tạo cùng nội dung logic và cùng fixture hash.

## Kiểm tra sau khi clone

Không cần 24 file nguồn lớn. Cài `pyarrow` rồi chạy:

```powershell
python .\tests\validate_fixtures.py
```

Lệnh phải kết thúc với `Fixture validation passed`.
