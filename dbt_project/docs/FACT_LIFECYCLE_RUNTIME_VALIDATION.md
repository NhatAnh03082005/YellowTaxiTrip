# Fact lifecycle và kiểm chứng runtime dbt

Ngày kiểm chứng: 17/11/2026. Phạm vi: dbt giai đoạn 0–4 trên PostgreSQL 18 và **dữ liệu mẫu cô lập**. Không chạy trên dữ liệu thật, không triển khai OLAP, Mage hoặc PostGIS. Hai database chỉ được tạo cho đợt kiểm chứng (`ck_bi_lifecycle_20261117`, `ck_bi_lifecycle_migration_20261117`) đã được xóa sau khi kiểm tra; truy vấn cuối xác nhận không còn database mẫu nào trong hai tên này. Kết quả dưới đây ghi nhận phiên bản rule trước khi người dùng yêu cầu bỏ ràng buộc năm; xem ghi chú cuối tài liệu.

## Thiết kế đã chọn

Fact chỉ chứa các source record hiện thuộc `ACCEPTED` hoặc `WARNING`. `source_record_key` là khóa upsert; `trip_key` vẫn là PostgreSQL identity. Chế độ mặc định `fact_lifecycle_mode: guard` dừng build nếu một Fact row không còn accepted. Chế độ `archive`, chỉ bật có chủ đích, lưu lịch sử trong `dq.fact_taxi_trip_history` (schema và tên bảng cấu hình được), rồi loại Fact row không còn hợp lệ. Mỗi sự kiện lưu source key, trip key nếu đã có, trạng thái cũ/mới, mã DQ, batch nếu có, thời điểm, loại thay đổi và ảnh Fact trước thay đổi. Raw, staging và quarantine không bị xóa.

| Phương án | Đánh giá |
|---|---|
| A. Soft delete trong Fact | Giữ identity đơn giản nhưng mọi truy vấn KPI phải lọc inactive; dễ đếm nhầm trong star schema hiện tại. |
| B. Fact hiện hành + lịch sử riêng | Giữ grain và KPI hiện tại; lưu được trạng thái cũ, ảnh Fact, identity để phục hồi. **Đã chọn.** |
| C. Bảng lifecycle độc lập + incremental | Linh hoạt hơn cho điều phối nhiều hệ thống, nhưng thêm đồng bộ và giao diện mới khi metadata Mage chưa hoàn chỉnh. |

Khi `ACCEPTED/WARNING → BUSINESS_QUARANTINE/DUPLICATE/TECHNICAL_REJECT`, materialization yêu cầu còn Raw và nhánh phân loại/technical, ghi sự kiện `DEACTIVATE` kèm ảnh Fact rồi xóa row khỏi Fact. Khi trở lại `ACCEPTED/WARNING`, nó tìm trip key lịch sử theo source key và insert bằng `OVERRIDING SYSTEM VALUE`; record chưa từng vào Fact nhận identity mới. Các thay đổi trạng thái trong Fact được ghi `STATUS_CHANGE`, thay đổi dữ liệu được ghi `DATA_UPDATE`. Record mới bị reject nhận `REJECTED` với trip key null, không tạo sự kiện lặp khi chạy lại. Bản Fact đã có trước khi bật archive được ghi `MIGRATE` một lần.

Lệnh trong materialization dùng một PostgreSQL transaction cho Fact và lịch sử, với advisory transaction lock để tuần tự hóa các writer. Lỗi sau khi ghi archive làm rollback cả archive và thay đổi Fact. **Toàn bộ `dbt build` không phải một transaction**: các model quarantine đã build trước vẫn có thể được cập nhật nếu Fact thất bại. Vì vậy chỉ công bố KPI và tăng watermark sau khi toàn bộ build và tests thành công.

## Thay đổi mã

- `dbt_project.yml`: mode, schema/tên history và công tắc fault injection chỉ dùng cùng `fixture_mode`.
- `macros/fact_identity_materialization.sql`: khóa ghi, archive/deactivation, lịch sử, phục hồi trip key, migration và guard.
- `models/marts/facts/fact_taxi_trip.sql`: khai báo dependency dùng trong materialization có nhánh điều kiện để dbt xếp đúng thứ tự build.
- `models/sources/taxi_sources.yml`: khai báo source history cấu hình được.
- `tests/singular/fact_lifecycle_history.sql`: kiểm chuỗi trạng thái, mapping trip key, Fact hiện hành và trạng thái reject; chỉ bật trong mode archive.
- `README.md` và báo cáo audit: hướng dẫn và kết quả kiểm chứng.

## Lệnh và kết quả thực tế

Môi trường thử dùng PostgreSQL 18 tại `127.0.0.1:55438`, `dbt-core 1.8.7`, `dbt-postgres 1.8.2`; đầu vào gồm hai Parquet 5.000 row, fixture nghiệp vụ 9 row và 5 technical rejects. Lệnh dbt dùng profile/database test tách biệt. Không ghi mật khẩu vào repository.

| Lệnh/kịch bản | Kết quả |
|---|---|
| `dbt deps`, `dbt parse`, `dbt compile` | Cả ba exit 0. Không có package ngoài để cài. |
| `dbt build --vars '{fixture_mode: true, fact_lifecycle_mode: archive}'` sau khi bổ sung lifecycle test | Exit 0, PASS 182, WARN 0, ERROR 0, SKIP 0: 17 models, 6 seeds, 10 sources, 159 tests. Fact 9.363 row; doanh thu 268.148,73. |
| Build lại cùng input | Exit 0, PASS 182; Fact count, tổng trip key và số sự kiện history giữ nguyên. |
| Build với `load_batch_id: 2` sau thêm/chỉnh mẫu | Exit 0, PASS 182 trong các lần chuyển trạng thái. |
| `dbt test` sau khôi phục fixture | Exit 0, PASS 159, WARN 0, ERROR 0, SKIP 0. |
| Migration từ Fact có sẵn ở mode guard sang archive, database mẫu thứ hai | Guard build exit 0, PASS 181 (158 tests). Archive build exit 0, PASS 182; 9.363 sự kiện `MIGRATE` có ảnh Fact, Fact count và tổng trip key không đổi. |
| Hai `dbt run --select fact_taxi_trip` archive chạy đồng thời | Cả hai exit 0, mỗi lần PASS 1; Fact 9.365 source key khác nhau và số sự kiện lịch sử không tăng sai. |
| Cố tình chạy guard sau khi có history | Exit 1 với lỗi giải thích phải dùng archive, không ghi Fact. |

Lần build ban đầu từng lỗi do dbt không thấy dependency `ref/source` trong nhánh materialization. Đã bổ sung khai báo dependency ở Fact; toàn bộ lệnh trên được chạy lại sau sửa và đạt kết quả nêu trong bảng.

## Các chuyển trạng thái và kiểm tra âm

| Tình huống mẫu | Kết quả quan sát |
|---|---|
| `ACCEPTED → WARNING → ACCEPTED` do passenger count null rồi khôi phục | Fact vẫn có row, `trip_key=852`, lịch sử ghi `STATUS_CHANGE` theo hai chiều. |
| `ACCEPTED → BUSINESS_QUARANTINE → ACCEPTED` do amount bằng 0 rồi khôi phục | Row rời Fact, vào business quarantine; sự kiện `DEACTIVATE` giữ trip key 852 và lý do; khi trở lại Fact dùng lại 852. |
| `ACCEPTED → DUPLICATE → ACCEPTED` trên row batch 2 | Duplicate quarantine nhận row, Fact loại row; sau chỉnh lại source, Fact dùng lại `trip_key=9366`. |
| `NEW → BUSINESS_QUARANTINE → ACCEPTED` với timestamp 1900 rồi sửa | Thời lượng trung gian 3.955.052.152 giây không tràn integer; history ghi reject với trip key null, sau sửa Fact nhận key mới 9367. |
| Update amount của row đã accepted | Fact cập nhật amount, giữ key 9367; lịch sử ghi `DATA_UPDATE` và ảnh Fact cũ. |
| Unknown Vendor | Fact ở `WARNING`, `vendor_key=0`, mã `CODE-001`, key 9367; sau khôi phục fixture build đạt. |
| Replay cùng source key | Hai lệnh nạp mẫu `ON CONFLICT DO NOTHING` đều insert 0 row; build không đổi Fact, KPI hay số sự kiện history. |
| Fault injection sau archive, trước delete/upsert | `dbt build` exit 1 (PASS 135, ERROR 1, SKIP 46). Fact row và số sự kiện history vẫn như trước lỗi: transaction Fact/history đã rollback. Sau bỏ lỗi, build PASS 182. |
| Sửa sai trip key trong history | Test `fact_lifecycle_history` FAIL 1; khôi phục rồi PASS. |
| Tạo lệch 1,00 ở raw technical amount | `dbt build --select file_reconciliation+` FAIL 1; khôi phục rồi full build PASS 182. |

Fixture cũng có passenger null, distance/duration 0, duration âm, amount âm, missing Zone, financial component warning và các row cùng nội dung nhưng khác source key/file. Các test phân nhánh, identity, KPI, FK và reconciliation đã chạy trong full build; chưa thể suy ra hành vi Mage thật từ fixture này.

## Đối soát cuối cùng

Sau khi khôi phục các thay đổi thử nghiệm, full build exit 0: PASS 182, WARN 0, ERROR 0, SKIP 0. Fact có 9.365 row và 9.365 source key duy nhất; tổng `trip_key=43.856.299`, tổng `total_amount=268.176,46`. History có 10.026 sự kiện trên 10.016 source key. `dq.file_reconciliation` có 4 file, tất cả chênh lệch số row nguồn và trusted đều 0; tất cả chênh lệch amount nguồn và trusted đều 0.

| File mẫu | Raw | Canonical | Fact | Business | Duplicate | Technical |
|---|---:|---:|---:|---:|---:|---:|
| Business fixture | 9 | 9 | 3 | 5 | 1 | 0 |
| Technical cast fixture | 5 | 0 | 0 | 0 | 0 | 5 |
| Parquet 2024 | 5.000 | 5.000 | 4.911 | 89 | 0 | 0 |
| Parquet 2025, gồm 2 row tổng hợp thêm | 5.002 | 5.002 | 4.451 | 551 | 0 | 0 |

`WARNING` là tập con của Fact, không cộng riêng vào phương trình raw = Fact + business + duplicate + technical. Replay/skipped không có metadata đầy đủ từ giao diện tạm để đối soát như một nhánh độc lập. Đối soát hiện theo file, chưa theo batch. Các lệnh sửa source trong phép thử mô phỏng thay đổi canonical trên database mẫu với cùng file SHA; pipeline Mage với file bất biến cần kiểm chứng riêng.

## Giới hạn và điều kiện tiếp theo

- Chưa chạy dữ liệu thật, tải dữ liệu toàn phạm vi 2024–2025, kiểm tra hiệu năng/lock và kích thước history ở quy mô đầy đủ. DDL history và chỉ mục được kiểm chứng trên mẫu, chưa đo thời gian migration production.
- Chưa có DDL thực tế của Mage Raw/technical quarantine, schema-fingerprint registry, cột PostGIS để xác minh SRID, metadata reason/batch/replay và cơ chế watermark. Kiểm chứng trên PostgreSQL 18 không thay cho xác minh phiên bản PostgreSQL đích, đặc biệt `pg_input_is_valid`.
- History được mã ghi thêm và test phát hiện sai lệch, nhưng quyền DB chưa khóa `UPDATE/DELETE` trực tiếp trên history. Cần backup và quyền ghi thích hợp trước vận hành thật.
- Khi bật `archive` trên database có Fact, sao lưu, kiểm tra schema hiện hữu và chạy migration có giám sát. Sau khi history tồn tại, mọi lần build Fact phải dùng archive mode; guard cố ý từ chối. Không dùng `--full-refresh` cho Fact identity.
- Trước OLAP: cần dùng Fact hiện hành, chỉ công bố KPI sau full build và tests PASS; kiểm chứng ratio-of-sums và các KPI OLAP trên dữ liệu đại diện.
- Trước tích hợp dữ liệu thật/Mage: chốt DDL nguồn, precision/hash, registry, technical reason và replay/batch metadata; kiểm tra PostGIS/SRID; định nghĩa retry và watermark chỉ tăng sau build thành công; thử migration, throughput, concurrency và reconciliation trên dữ liệu đại diện.

Các log/target chi tiết của đợt chạy nằm trong thư mục `%TEMP%\ck_bi_phase2_check\` của máy kiểm chứng; database mẫu đã xóa. Các phép chuyển trạng thái được thao tác trong database mẫu và ghi bằng chứng ở tài liệu này, chưa có integration runner tái tạo tự động.

**Điều chỉnh sau kiểm chứng:** Rule `TIME-006` (pickup ngoài 2024–2025)
đã bị bỏ theo yêu cầu người dùng; hai năm này chỉ xác định tập file team nạp.
Kết quả build và số liệu trong tài liệu này là bằng chứng của code trước lần
điều chỉnh, không được dùng làm bằng chứng runtime cho thay đổi mới.

Kiểm chứng bổ sung sau điều chỉnh: dùng database cô lập
`ck_bi_scope_clarification_20261117`, nạp fixture cũ và thêm một dòng tổng hợp
trong file 2025-05 có pickup/dropoff hợp lệ ở 2026-05. `dbt build` chế độ
`guard` đạt **PASS 181, WARN 0, ERROR 0, SKIP 0** trước và sau khi thêm dòng.
Dòng mới nhận duy nhất `TIME-005`, trạng thái `WARNING`, có mặt trong Fact;
`dq.file_reconciliation` không có dòng sai lệch. Database này đã được xóa,
truy vấn xác nhận còn 0 database trùng tên, và PostgreSQL mẫu đã dừng. Chế độ
`archive` với rule mới chưa được chạy lại; kết quả archive phía trên thuộc
phiên bản rule trước điều chỉnh.
