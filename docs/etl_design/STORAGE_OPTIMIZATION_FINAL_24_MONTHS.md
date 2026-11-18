# Đề xuất cuối cùng tối ưu lưu trữ và ETL Yellow Taxi 2024–2025

**Ngày chốt đề xuất:** 18/11/2026  
**Trạng thái:** `FINAL PROPOSAL — CHỜ PROJECT LEAD VÀ DBT OWNER KÝ DUYỆT`  
**Phạm vi:** Mage, PostgreSQL/PostGIS, Staging, dbt DDS/DQ/OLAP, Control và Docker local  
**Mục tiêu:** xử lý đủ 24 file từ `2024-01` đến `2025-12` trên máy local mà không làm mất grain, lineage, DQ, khả năng retry, reconciliation hoặc yêu cầu phân tích của đồ án.

---

## 1. Quyết định chốt

Chọn kiến trúc:

> **Parquet nguồn bất biến + Staging Raw/Canonical ngắn hạn theo một file + Fact/DQ tích lũy incremental + metadata, fingerprint history và reconciliation lưu lâu dài.**

Không chọn một trong hai cực sau:

1. Không giữ Raw và Canonical của đủ 24 tháng trong PostgreSQL, vì dung lượng ngoại suy vượt khả năng local trước khi cộng Fact, DQ, OLAP, WAL và temp.
2. Không xóa Raw/Canonical ngay trên code hiện tại, vì dbt còn phụ thuộc chúng cho fingerprint, lifecycle, duplicate và reconciliation.

Việc chuyển Staging sang vòng đời ngắn chỉ được bật sau khi các thay thế bền vững ở mục 5 đã hoàn thành và vượt qua pilot.

---

## 2. Căn cứ và các bất biến bắt buộc

Thiết kế cuối phải giữ:

- Đủ 24 file `yellow_tripdata_YYYY-MM.parquet`, từ `2024-01` đến `2025-12`.
- Luồng `Source → Staging → DDS → OLAP/Power BI`.
- Fact ở grain một source trip record `ACCEPTED` hoặc `WARNING`.
- Sáu dimension và role-playing Date/Time/Zone theo Design Freeze.
- `source_record_key` ổn định qua retry; không dùng fingerprint làm unique business key của Fact.
- Raw-exact duplicate trong cùng file: chỉ giữ source row nhỏ nhất; các dòng sau vào duplicate quarantine.
- Fingerprint trùng giữa các file: warning, không tự động xóa chuyến.
- Technical, business và duplicate quarantine có đủ lý do và khả năng truy nguyên dòng nguồn.
- Hai phương trình reconciliation chính xác theo từng file/version.
- Watermark chỉ tiến sau DDS, DQ, tests, reconciliation và OLAP thành công.
- Có thể rebuild từ Parquet đã kiểm checksum.
- Dashboard đọc DDS/OLAP, không đọc trực tiếp file nguồn.

Seminar cho phép Staging dùng chiến lược append hoặc replace tạm. Vì vậy Staging ngắn hạn không vi phạm kiến trúc ba tầng, miễn là dữ liệu thực sự đi qua PostgreSQL Staging và mọi bằng chứng lịch sử cần thiết đã được lưu trước cleanup.

---

## 3. Số liệu dùng để ra quyết định

### 3.1. Phạm vi nguồn thực tế

Đo từ Parquet footer của 24 file hiện có:

| Chỉ số | Giá trị |
| --- | ---: |
| Số file | 24 |
| Tổng source rows | 89.892.322 |
| Tổng dung lượng Parquet | khoảng 1,418 GiB |

Không dùng ngoại suy 99,8 triệu dòng từ hai tháng làm số kế hoạch chính thức nữa.

### 3.2. Baseline PostgreSQL tháng 2024-05

| Quan hệ | Heap + TOAST | Index | Tổng |
| --- | ---: | ---: | ---: |
| `stage.yellow_taxi_raw` | 1.884.127.232 B | 1.012.973.568 B | 2.897.100.800 B |
| `stage.yellow_taxi_canonical` | 1.685.561.344 B | 752.730.112 B | 2.438.291.456 B |
| **Tổng hai bảng** | **3.569.688.576 B** | **1.765.703.680 B** | **5.335.392.256 B** |

Tháng 2024-05 có 3.723.833 rows, tương đương khoảng 1.433 byte/source row cho riêng Raw + Canonical và index.

Nếu độ rộng tương tự được áp dụng cho 89.892.322 rows:

- Raw + Canonical có thể xấp xỉ **128,8 GB**.
- Chỉ Canonical vẫn có thể xấp xỉ **58,9 GB**.
- Các con số trên chưa gồm Fact, persistent DQ, OLAP, Control, WAL, temp spill và headroom.

Kết luận: chỉ dọn Raw nhưng giữ toàn bộ Canonical không phải đích kiến trúc đủ an toàn cho 24 tháng.

---

## 4. Kiến trúc lưu trữ cuối cùng

```text
24 Parquet bất biến + SHA-256 + schema registry
                    |
                    v
       Mage: một source file / một child batch
       - bounded Arrow batches
       - stable source identity
       - technical validation
                    |
                    v
       PostgreSQL Raw + Canonical Staging
       - chỉ file/version đang xử lý
       - không dùng session TEMP TABLE
                    |
                    v
       dbt fingerprint + DQ + dimensions + Fact
                    |
          +---------+----------+
          |                    |
          v                    v
 Persistent DQ        Persistent Fact/Dims
 Fingerprint history  + OLAP cần thiết
 Reconciliation
                    |
                    v
       Tests + per-file reconciliation
                    |
                    v
       Commit SUCCESS + watermark
                    |
                    v
       Cleanup Staging/work files có log
                    |
                    v
              File tiếp theo
```

### Chính sách từng lớp

| Thành phần | Chính sách cuối |
| --- | --- |
| Parquet nguồn | Giữ bất biến lâu dài; checksum và schema version bắt buộc |
| Raw Staging | Ngắn hạn, tối đa một file/version đang xử lý |
| Canonical Staging | Ngắn hạn, tối đa một file/version đang xử lý |
| Reference Staging | Giữ lâu dài vì nhỏ |
| Technical quarantine | Giữ lâu dài payload lỗi cast và lineage |
| Business/duplicate quarantine | Incremental, idempotent, cột được chọn rõ ràng |
| Fact/Dimensions | Giữ lâu dài; Fact upsert incremental theo source identity |
| Fingerprint history/index | Dùng fingerprint đã lưu và index trong Fact/DQ; bảng riêng chỉ lưu cross-file match thực sự |
| Reconciliation history | Giữ lâu dài theo source file version |
| OLAP | Chỉ materialize aggregate/dashboard thực sự sử dụng |
| Work Parquet | TTL sau full SUCCESS; giữ khi STAGED/FAILED |
| dbt test failures | Không lưu đại trà hoặc vô hạn |

---

## 5. Các thay đổi bắt buộc trước khi cleanup Staging

### 5.1. Source file version ổn định

Tạo `control.source_file_version`:

```text
file_version_id BIGINT PK
source_file
source_year_month
source_file_sha256
schema_fingerprint
schema_version
source_last_modified
content_length
source_url
verified_at
UNIQUE (source_file, source_file_sha256)
```

`control.etl_file` tiếp tục đại diện cho một attempt trong batch và tham chiếu `file_version_id`. Không dùng `etl_file_id` làm file-version identity.

Các bảng dòng lớn giữ `file_version_id`, `source_row_number`, `source_record_key`, `batch_id` và các cột cần thiết. Metadata chuỗi cấp file được trả lại qua lineage view khi dbt, audit hoặc báo cáo cần.

Giữ nguyên công thức `source_record_key` trong giai đoạn này.

### 5.2. Fingerprint lịch sử qua nhiều file

Không tạo một registry mới lặp lại toàn bộ 89,9 triệu source records. Chốt triển khai:

1. Fact, business quarantine và duplicate quarantine đều giữ `raw_exact_fingerprint` và `candidate_trip_fingerprint` lâu dài.
2. Tạo index thích hợp trên hai fingerprint ở từng nhánh sau khi benchmark index đơn/partial index.
3. Tạo `audit.v_trip_fingerprint_history` bằng `UNION ALL` ba nhánh trên, trả về source key, file version, source row, hai fingerprint và disposition.
4. Tạo `audit.cross_file_fingerprint_match` chỉ lưu các fingerprint thực sự xuất hiện ở hơn một file version, loại fingerprint, các file version liên quan và thời điểm phát hiện.
5. Khi xử lý file mới, so fingerprint hiện tại với history của **file version khác**. Current row và các historical row liên quan đều phải quan sát được `DUP-002`; có thể cập nhật affected rows hoặc expose qua một classification/audit view đã được test tương đương.

Same-file duplicate vẫn được rank trên toàn bộ file hiện tại bằng `source_file_sha256 + raw_exact_fingerprint`, survivor là `source_row_number` nhỏ nhất. Không dùng bảng cross-file match để quarantine record.

Không được chỉ query `int_taxi_trip_fingerprinted` từ Staging hiện tại vì sau cleanup cách đó không còn thấy lịch sử. Golden tests phải chứng minh `raw_exact_fingerprint` và `candidate_trip_fingerprint` khớp bit-for-bit trước và sau refactor trên schema 2024 và 2025.

### 5.3. Fact lifecycle theo processing scope

Mọi stale/deactivate/reclassification chỉ được xét trong `file_version_id` hoặc affected-key set đang xử lý.

Không được so toàn bộ Fact lịch sử với Staging của một file. Custom materialization hiện tại phải được sửa và test ở cả `guard` lẫn `archive` trước khi bật cleanup.

Khi một file cùng tên có checksum mới:

1. Dừng để source-version review.
2. Tạo/duyệt file version mới.
3. Xác định tập source keys thuộc version cũ và mới.
4. Upsert/deactivate đúng scope.
5. Không ghi đè âm thầm hoặc làm stale các tháng khác.

### 5.4. DQ persistent và khả năng tái tạo dòng

Không dùng `SELECT *` cho business/duplicate quarantine. Tối thiểu giữ:

- `source_record_key`, `file_version_id`, `source_row_number`, `batch_id`.
- Disposition, severity và `dq_rule_ids`.
- Raw/candidate fingerprint cần cho duplicate history.
- Survivor key và occurrence number cho duplicate.
- Các trường amount cần reconciliation.
- Các business values cần điều tra mà không buộc scan toàn bộ nguồn.

Khả năng truy lại dòng đầy đủ phải được chứng minh bằng một trong hai cách:

1. DQ giữ các business columns cần audit; hoặc
2. DQ giữ locator xác định và công cụ/runbook đọc đúng row từ Parquet version bất biến.

Nếu file nguồn có version mới, version cũ phải còn khả năng replay; locator trỏ đến file đã bị overwrite không được coi là lineage hợp lệ.

### 5.5. Reconciliation hiện tại và lịch sử

Tách:

- `current_file_reconciliation`: tính từ Staging hiện tại.
- `reconciliation_history`: upsert theo `file_version_id` và successful run semantics.
- `global_reconciliation_24_files`: xác nhận đủ 24/24 version và tổng hợp toàn phạm vi.

Phải giữ:

```text
source/raw rows = canonical rows + technical quarantine rows

canonical rows = Fact accepted/warning
               + business quarantine
               + same-file duplicate loser
```

Amount reconciliation chỉ tính trên dòng cast-eligible, theo precision và tolerance đã duyệt.

### 5.6. dbt incremental thực sự

- Business/duplicate quarantine: incremental và idempotent.
- Fact: upsert theo `source_record_key`, chỉ xử lý current/affected set.
- Reconciliation history: incremental theo file version.
- Intermediate: view/ephemeral hoặc bảng hẹp có invalidation rõ ràng; không materialize bản sao rộng không cần thiết.
- OLAP: incremental aggregate hoặc rebuild nhỏ theo dashboard; tránh full CUBE bùng nổ tổ hợp.
- Không bật `--store-failures` mặc định cho mọi build.

### 5.7. Cleanup an toàn

Thứ tự một child batch:

```text
DISCOVERY
→ FILE_VALIDATION
→ STAGING
→ DBT_BUILD_DDS_DQ
→ DBT_TEST
→ RECONCILIATION
→ DBT_BUILD_OLAP
→ COMMIT SUCCESS / WATERMARK
→ CLEANUP STAGING + WORK FILES
```

Cleanup là maintenance idempotent có log. Nếu cleanup thất bại:

- Batch đã SUCCESS không bị đổi thành thất bại dữ liệu.
- Không xử lý file tiếp theo trên cùng Staging.
- Retry cleanup trước khi tiếp tục.
- Không xóa dữ liệu của batch/file khác.

Chỉ `TRUNCATE` khi hệ thống bảo đảm một writer và không còn consumer của Staging. Nếu cần concurrency sau này phải chuyển sang cleanup scoped bằng partition/table-swap; không dùng `TRUNCATE` chung.

---

## 6. Các tối ưu bổ sung được chấp thuận

### Làm trong đợt đầu

- Chuẩn hóa metadata bằng `file_version_id`.
- Bỏ `SELECT *` khỏi DQ.
- Đọc trực tiếp raw columns thay vì dựng `to_jsonb(r)` cho mọi dòng.
- Rà index bằng `EXPLAIN (ANALYZE, BUFFERS)` và size/usage thật.
- Pilot bỏ/thay index `(source_file_sha256, source_row_number)` nếu workload chứng minh không cần.
- Dọn work files sau SUCCESS.
- Giới hạn batch đọc Parquet theo RAM thực tế.
- Đo WAL, temp spill và peak disk, không chỉ final database size.

### Có thể pilot sau

- `UNLOGGED` cho Raw/Canonical Staging vì có thể replay từ Parquet. Không dùng cho Fact, DQ, Control hoặc reconciliation.
- Lưu hash dưới dạng `bytea(32)` trong các bảng nội bộ mới, kèm view hex tương thích.
- Mã hóa các chuỗi lặp trong Fact như severity/band/disposition thành code nhỏ và expose tên qua view.
- Partition Fact chỉ khi benchmark chứng minh lợi ích và đã giải quyết uniqueness/lifecycle; partition không được coi là biện pháp giảm byte mặc định.

### Không chấp thuận

- Giảm xuống dưới 24 tháng hoặc lấy mẫu cho trusted Fact.
- Dùng pickup year để loại record ngoài 2024–2025; 2024–2025 là phạm vi file nguồn.
- Hash Canonical đã round thay cho raw-exact fingerprint.
- Loại cross-file duplicate.
- Bỏ quarantine, reconciliation hoặc tests để tiết kiệm dung lượng.
- Drop unique `source_record_key`.
- Cleanup khi batch mới chỉ `STAGED` hoặc downstream chưa hoàn tất.
- Full-refresh 90 triệu rows trong mỗi child batch.
- Reset PostgreSQL/volume trước khi code V2 và pilot đã sẵn sàng.

---

## 7. Kế hoạch triển khai có cổng kiểm soát

### Giai đoạn 0 — Hoàn tất baseline hiện tại

- Hoàn thiện batch 2024-05 từ `STAGED` đến Fact, DQ, reconciliation, OLAP và watermark.
- Không xóa Raw của batch hiện tại khi chưa có Fact/OLAP và chưa commit SUCCESS.
- Đo table/index/TOAST/WAL/temp và thời gian từng bước.

**Gate:** một tháng end-to-end SUCCESS, dashboard đọc DDS/OLAP, rerun không tăng Fact.

### Giai đoạn 1 — Giảm sao chép ít rủi ro

- Thu hẹp DQ.
- Điều chỉnh store-failures.
- Pilot index lớn.
- Dọn work files thành công cũ.

**Gate:** identity, DQ, reconciliation và KPI không đổi; runtime không suy giảm ngoài ngưỡng nhóm chấp nhận.

### Giai đoạn 2 — Xây persistent interfaces

- Tạo `source_file_version` và lineage views.
- Tạo fingerprint history view, indexes và bảng cross-file match.
- Tạo reconciliation history.
- Sửa Fact lifecycle và incremental DQ theo scope.

**Gate:** kết quả bit-for-bit hoặc business-equivalent trên một file 2024 và một file 2025; cross-file warning và file-version replacement test đạt.

### Giai đoạn 3 — Bật Staging ngắn hạn

- Chạy 2024-05 và 2025-05 theo mô hình một file/batch.
- Test failure ở dbt, tests, reconciliation và cleanup.
- Test replay từ Parquet sau cleanup.
- Đo Fact bytes/row, DQ bytes/row và peak disk.

**Gate:** forecast cho 89.892.322 rows nằm dưới dung lượng khả dụng với tối thiểu 25% headroom.

### Giai đoạn 4 — Full backfill

- Chạy lần lượt 2024-01 đến 2025-12, không overlap Staging.
- Checkpoint ở 1, 2, 6, 12, 18 và 24 file.
- Dừng nếu forecast vượt ngân sách.
- Sau file cuối chạy global reconciliation, uniqueness/FK tests, KPI và Power BI validation.

**Gate cuối:** 24/24 file SUCCESS; tất cả tests/reconciliation đạt; rerun một file không tạo trùng và giữ ổn định `trip_key`.

---

## 8. Bộ kiểm thử nghiệm thu bắt buộc

| Mã | Kiểm thử | Điều kiện PASS |
| --- | --- | --- |
| FIN-01 | Source scope | Đủ 24 file và 89.892.322 source rows theo footer hiện tại |
| FIN-02 | Technical split | Raw/source = canonical + technical quarantine theo file version |
| FIN-03 | Trusted split | Canonical = Fact + business DQ + duplicate loser |
| FIN-04 | Same-file duplicate | Survivor là source row nhỏ nhất |
| FIN-05 | Cross-file match | Warning, không tự loại record |
| FIN-06 | Retry | Không tăng Fact/DQ, không đổi `trip_key` ngoài lifecycle hợp lệ |
| FIN-07 | Downstream failure | Watermark không tiến; Staging không bị cleanup sớm |
| FIN-08 | Cleanup retry | Không xóa nhầm và không xử lý file tiếp theo khi cleanup tồn đọng |
| FIN-09 | Replay | Rebuild được từ đúng Parquet version/checksum |
| FIN-10 | File version change | Có approval và chỉ thay đổi đúng scope |
| FIN-11 | Schema evolution | v001/v002 và CBD fee được xử lý đúng |
| FIN-12 | KPI | Revenue, tip rate, speed và eligibility khớp contract |
| FIN-13 | DDS | Unique grain, not-null keys, không orphan FK |
| FIN-14 | OLAP/BI | May/June, MoM/YoY, routes, map và drill-down hoạt động |
| FIN-15 | Disk safety | Peak dưới ngân sách và còn ít nhất 25% headroom |

---

## 9. Parent run và child batch

Full backfill cần một parent run đại diện cho phạm vi 2024–2025 và 24 child batches, mỗi child batch xử lý một file/version.

Child batch chỉ chứng minh file đó thành công. Parent run chỉ SUCCESS khi:

- Catalog đúng 24/24 file.
- Mọi child batch required steps đều SUCCESS.
- Không còn cleanup pending.
- Global reconciliation đạt.
- Final DDS/OLAP/BI validation đạt.

Không đổi `expected_count` thành 1 rồi coi mỗi child batch là hoàn thành toàn bộ backfill.

---

## 10. Sign-off và quản trị thay đổi

Tài liệu này là Decision Log V2, không sửa im lặng Design Freeze cũ. Khi ký duyệt phải cập nhật đồng bộ:

- `docs/mapping/DBT_HANDOVER_CONTRACT.md`.
- `docs/etl_design/CONTROL_METADATA_DATA_DICTIONARY.md`.
- Staging DDL/index/tests.
- Mage controller, loader và cleanup.
- dbt Fact/DQ/reconciliation/tests/OLAP.
- Runbooks, evidence và kế hoạch dự án.

| Vai trò | Quyết định | Ngày |
| --- | --- | --- |
| Project Lead / Integration | PENDING | — |
| dbt Owner | PENDING | — |

**Định nghĩa hoàn thành:** hệ thống xử lý đủ 24 tháng trên local bằng Staging một file, giữ đúng kết quả nghiệp vụ và lineage, có bằng chứng retry/replay/reconciliation, và peak disk nằm trong ngân sách đã đo.
