# dbt — NYC Yellow Taxi

> Cập nhật 2026-11-19: Cụm 2 đã có DDS tối giản **opt-in**; xem [implementation guide](../docs/etl_design/DBT_MINIMAL_CLUSTER2_IMPLEMENTATION.md). Mặc định vẫn legacy. Chưa chuyển live; runner/tests/reconciliation/OLAP mới thuộc Cụm 3. Các phần lifecycle/archive dưới đây mô tả luồng legacy, không phải yêu cầu của mode tối giản.

Dự án dbt biến dữ liệu taxi đã được kiểm tra kiểu dữ liệu thành các bảng chất lượng (`dq`) và mô hình phân tích (`dds`). Phạm vi file team chọn là 2024–2025; dbt **không loại chuyến chỉ vì ngày pickup nằm ngoài hai năm đó**. Quy tắc hiện hành được đối chiếu với [hợp đồng bàn giao](../docs/mapping/DBT_HANDOVER_CONTRACT.md).

## Trạng thái hiện tại

- Đã xây dựng và chạy thử trên dữ liệu mẫu: staging, tính toán trung gian, phân loại chất lượng, sáu dimensions, Fact, lịch sử Fact, đối soát theo file và dbt tests.
- Chưa tích hợp pipeline Mage thật, DDL PostgreSQL/PostGIS thật hoặc kiểm thử đủ 24 file. Chưa có ba model OLAP; chúng thuộc bước tiếp theo.
- Kết quả chạy thử và các giới hạn được ghi trong [báo cáo đối chiếu](docs/DBT_AUDIT_AND_REFACTOR_REPORT.md) và [báo cáo Fact lifecycle](docs/FACT_LIFECYCLE_RUNTIME_VALIDATION.md). Database mẫu của các lần kiểm chứng đã được xóa.

## Luồng dữ liệu và trách nhiệm

```text
Mage (dự kiến): file nguồn → stage.yellow_taxi_raw
                        → stage.yellow_taxi_canonical
                        → technical quarantine (dòng lỗi chuyển kiểu)

dbt: canonical + raw → staging → enriched → fingerprinted → classified
                                              ├→ business quarantine
                                              ├→ duplicate quarantine
                                              └→ accepted/warning → 6 dimensions + fact_taxi_trip
     raw + canonical + technical + Fact + quarantine → file_reconciliation
```

Mage sở hữu bảng nguồn vật lý, tải file, checksum, technical rejects và watermark. dbt không tạo các bảng `stage` hoặc bảng điều khiển của Mage. Các source dbt đang cần được khai báo trong [taxi_sources.yml](models/sources/taxi_sources.yml), gồm `stage.yellow_taxi_raw`, `stage.yellow_taxi_canonical`, `stage.taxi_zone_raw`, `stage.taxi_zone_geometry`, `control.etl_batch` và giao diện technical quarantine. Giao diện technical tạm mặc định là `dq.technical_quarantine`; có thể đổi schema/tên bảng trong `dbt_project.yml`.

Raw phải giữ đủ giá trị nguồn và độ chính xác để tính fingerprint. `source_record_key` xác định dòng nguồn; hai fingerprint chỉ dùng để phát hiện trùng, không làm khóa duy nhất của Fact. Với lỗi kỹ thuật, dòng vẫn phải nằm trong Raw và technical quarantine, nhưng không nằm trong canonical.

## Cấu hình

- [dbt_project.yml](dbt_project.yml): ngưỡng DQ, ngày hiệu lực CBD, danh sách mã kỳ vọng, Zone, các nhóm phân tích, biến batch và chế độ Fact.
- [dq_rule_policy.csv](seeds/dq_rule_policy.csv): mức độ và nhánh xử lý cho từng mã quy tắc. Các seed còn lại ánh xạ Vendor, Payment, Rate và Zone. Đổi một surrogate Zone đã dùng cần migration có kiểm soát để giữ đúng khóa trong Fact.
- `fixture_mode: false` là mặc định. Chỉ bật `fixture_mode: true` khi đã nạp fixture mẫu; chế độ này bật seed và test kỳ vọng của chín dòng nghiệp vụ.
- `load_batch_id` là tùy chọn. Khi đặt biến này, Fact xử lý batch được chọn và xét lại dòng cũ có fingerprint khớp batch; các model phân loại và đối soát vẫn nhìn dữ liệu canonical đã nạp.

Các ngưỡng có thể sửa ở cấu hình và hành động của rule có thể sửa ở seed. Khi thay chính sách, chạy lại seed, các model bị ảnh hưởng và tests. Điều kiện SQL của một loại rule mới vẫn cần chỉnh model và bổ sung kiểm chứng.

## Chuẩn bị trước khi chạy

1. Có PostgreSQL và các bảng nguồn đúng contract. Hiện repository chưa có pipeline Mage/DDL thật để tự nạp chúng; muốn chạy mẫu phải nạp fixture vào một database riêng trước.
2. Cài `dbt-core` và `dbt-postgres` tương thích. Các lượt kiểm chứng mẫu dùng phiên bản 1.8.7 và 1.8.2; đây chưa phải bộ phiên bản đã chốt cho môi trường chung.
3. Đặt `profiles.yml` trong một thư mục cấu hình riêng dựa trên [profiles.yml.example](profiles.yml.example). Profile dùng `DBT_HOST`, `DBT_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`; không ghi mật khẩu vào repository.
4. Từ thư mục `dbt_project`, kiểm tra kết nối rồi build. `dbt build` chạy seeds, models và tests theo dependency.

```powershell
cd dbt_project
$env:DBT_HOST = '127.0.0.1'
$env:DBT_PORT = '5432'
$env:POSTGRES_USER = '<user>'
$env:POSTGRES_PASSWORD = '<password>'
$env:POSTGRES_DB = '<database đã nạp nguồn>'
dbt debug --profiles-dir '<thư mục chứa profiles.yml>'
dbt deps --profiles-dir '<thư mục chứa profiles.yml>'
dbt parse --profiles-dir '<thư mục chứa profiles.yml>'
dbt build --profiles-dir '<thư mục chứa profiles.yml>'
```

`dbt deps` hiện không cần tải package bên ngoài. Với database mẫu **đã nạp fixture**, chạy `dbt build --profiles-dir '<thư mục chứa profiles.yml>' --vars '{fixture_mode: true, fact_lifecycle_mode: archive}'` để kiểm tra cả lịch sử Fact. Không chạy `--full-refresh` cho `fact_taxi_trip`, vì việc tạo lại bảng sẽ cấp lại `trip_key`.

## Fact và chuyển trạng thái

Fact chỉ giữ các dòng hiện thuộc `ACCEPTED` hoặc `WARNING`. `trip_key` là PostgreSQL identity; `source_record_key` duy nhất là khóa upsert. Chạy lại cùng nguồn không tạo thêm chuyến. Sáu dimensions là Date, Time, Zone, Vendor, Payment Type và Rate Code; khóa kỹ thuật `0` là Unknown, `-1` là Not Applicable.

`fact_lifecycle_mode` có hai giá trị:

| Mode | Cách hoạt động |
|---|---|
| `guard` (mặc định) | Dừng nếu một dòng đang ở Fact không còn accepted/warning. Dùng trước khi quyết định migration lịch sử. |
| `archive` | Ghi thay đổi vào `dq.fact_taxi_trip_history`, loại dòng hết hợp lệ khỏi Fact và khôi phục `trip_key` cũ nếu dòng được chấp nhận lại. Bật bằng `--vars '{fact_lifecycle_mode: archive}'`. |

Lần đầu bật `archive` trên Fact đã có dữ liệu sẽ ghi sự kiện `MIGRATE` cho các khóa cũ. Khi bảng lịch sử đã tồn tại, các lần build Fact sau phải tiếp tục dùng `archive`; `guard` sẽ từ chối để tránh cập nhật không được ghi lịch sử. Cần kiểm tra schema và sao lưu database thật trước migration. Fact và lịch sử được cập nhật trong một PostgreSQL transaction, nhưng **toàn bộ `dbt build` không phải một transaction**. Chỉ công bố KPI hoặc tăng watermark sau khi build và tests đều đạt.

## Đối soát và xem kết quả test

`dq.file_reconciliation` kiểm theo từng file:

```text
Raw = Canonical + Technical quarantine
Canonical = Fact + Business quarantine + Duplicate quarantine
```

Số dòng phải khớp chính xác. Tổng tiền chỉ so trên dòng chuyển được sang `numeric(12,2)` và cho phép chênh lệch tối đa `amount_reconciliation_tolerance` (mặc định 0,01 USD/file). `WARNING` đã nằm trong Fact nên không cộng thêm một nhánh. Đối soát replay/skipped và theo batch cần metadata Mage thật.

`dbt build` in trạng thái từng test. Muốn xem thông tin chi tiết và tối đa năm dòng lỗi đã lưu, dùng script từ thư mục `dbt_project` (script chạy `dbt build --store-failures` và giữ nguyên exit code):

```powershell
python scripts/build_with_detailed_tests.py --profiles-dir '<thư mục chứa profiles.yml>' --vars '{fixture_mode: true, fact_lifecycle_mode: archive}'
```

`--store-failures` tạo bảng lỗi trong schema `dbt_test__audit`. Có thể đổi số dòng hiển thị bằng `--max-failure-rows N`. Chỉ bật `fixture_mode` khi nguồn mẫu đã được nạp.

## Giới hạn trước tích hợp thật

- DDL Mage cho Raw, technical quarantine, registry schema fingerprint và geometry chưa được cung cấp để xác nhận độ chính xác Raw, phiên bản schema, SRID và trường reason/batch/replay. SRID kỳ vọng theo thiết kế là 2263 nhưng chưa có test cột geometry thật.
- Việc nhận biết CBD thiếu theo cấu trúc hiện dựa trên tháng file; cần đối chiếu với registry schema version của Mage khi tích hợp.
- Chưa có bảng theo dõi P99 hoặc model OLAP. Các KPI tổng hợp như tip rate và speed cần tính bằng tỷ số của các tổng trên tập dòng đủ điều kiện, không lấy trung bình các tỷ số từng chuyến.
- Các kết quả PASS hiện có chỉ áp dụng cho fixture trong database cô lập. Không suy ra hiệu năng hoặc tính đúng đắn của lượt nạp đủ 24 file và dữ liệu thật.