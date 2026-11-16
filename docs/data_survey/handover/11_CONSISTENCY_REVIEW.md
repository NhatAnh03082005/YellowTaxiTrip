# 11. Báo cáo kiểm tra tính nhất quán

**Phạm vi kiểm tra:** README, 11 file Python, requirements, raw/reference, log, output 01–12 và báo cáo cuối tại trạng thái ngày 2026-10-30. Không sửa code/output trong quá trình review.

## 1. Kết quả đối chiếu tự động

| Kiểm tra | Kết quả |
|---|---|
| Tên 7 raw file so với `representative_files.csv` | Khớp hoàn toàn |
| `downloaded_size_bytes` so với file system | Khớp 7/7 |
| `02.size_bytes` so với raw | Khớp 7/7 |
| `02.total_rows` so với Parquet footer | Khớp 7/7 |
| SHA-256 tính lại so với representative inventory | Khớp 7/7 |
| Tổng dòng profile/report | 60.108.247, khớp |
| Tổng dung lượng raw | 1.453.385.292 byte, khớp profile |
| File đại diện report so với raw | Khớp: 2009-01, 2010-01, 2015-01, 2019-01, 2024-04, 2025-01, 2026-01 |
| Output 01–12 | Đều tồn tại, non-zero |
| `11_anomaly_samples.parquet` | Đọc được, 404 dòng |
| Rule ID registry | 40 ID duy nhất; không duplicate ID trong `09` |

## 2. README nhắc file không tồn tại

Không phát hiện. Các input/output được README nêu đều tồn tại. README nói project brief PDF là optional và file hiện có.

## 3. File tồn tại nhưng README chưa mô tả rõ

| Vấn đề | Bằng chứng | Ảnh hưởng | Đề xuất |
|---|---|---|---|
| README chỉ gom `01...08`, `09...11`, không giải thích ba output phụ | Có `distribution_percentiles.csv`, `legacy_spatial_summary.csv`; `representative_files.csv` chỉ được nhắc ở phần input/cache | Low: người mới dễ bỏ qua evidence quan trọng | Bổ sung mục “supporting outputs” trong README |
| `project_brief_extracted.txt` không được README nhắc | File có trong `data/reference`; không có script project tạo | Low | Ghi rõ đây là artifact hỗ trợ, không phải source chính |
| Shapefile extracted components không được liệt kê cụ thể | Thư mục có `.shp/.shx/.dbf/.prj/.cpg` | Low | Ghi rằng `profile_shapefile.py` có thể giải nén |
| `data/sample/` tồn tại nhưng trống/không dùng | `common.py:12,24`; không script nào đọc/ghi | Info | Xóa khỏi kiến trúc tương lai hoặc ghi “reserved” (không sửa trong review này) |

## 4. Khác biệt code, README và báo cáo

### C-01 — Báo cáo mô tả output spatial ở mức từng dòng nhưng code chỉ xuất aggregate

- **Bằng chứng:** Report nói ghi `spatial_match_count`, `derived_location_id` (`generate_deliverables.py:151`). `profile_legacy_spatial.py:42–49` chỉ tạo `legacy_spatial_summary.csv`, cập nhật rule summary và sample; không ghi per-trip mapping.
- **Mức ảnh hưởng:** High nếu nhóm hiểu nhầm đã có LocationID cho mọi trip 2009/2010.
- **Đề xuất:** Đổi câu báo cáo thành “thiết kế đề xuất” hoặc bổ sung ETL xuất bảng per-trip native/derived LocationID.

### C-02 — `record_hash` mới là đề xuất, chưa được code tạo

- **Bằng chứng:** Report liệt kê `record_hash` trong Staging (`generate_deliverables.py:173`); tìm trong pipeline không có phép hash record, chỉ có SHA-256 file raw.
- **Mức ảnh hưởng:** Medium: có thể nhầm file checksum với record hash.
- **Đề xuất:** Gắn nhãn rõ “future Staging field”; khi triển khai phải chốt canonical serialization/hash algorithm.

### C-03 — README gọi 10 lệnh chạy nhưng project có 11 file Python

- **Bằng chứng:** README dòng 27–36 có 10 executable; `common.py` là file Python thứ 11 nhưng không phải executable.
- **Mức ảnh hưởng:** Info; không phải thiếu bước.
- **Đề xuất:** Ghi “10 executable scripts + 1 shared module”.

## 5. Output path và dependency

Không phát hiện script ghi sai thư mục so với code path constants. `common.py:8–18` dùng path tương đối theo project root, không phụ thuộc `D:\BI\DATH`.

Tuy nhiên:

### C-04 — Legacy spatial không idempotent

- **Bằng chứng:** `profile_legacy_spatial.py:44–49` luôn append DQ-SPATIAL rows và samples vào `10/11`, không xóa phiên bản cũ.
- **Ảnh hưởng:** High khi rerun riêng; summary/sample bị nhân đôi, sau đó `09` aggregate sai.
- **Đề xuất:** Trước append, xóa rows cùng `(file, role/rule)` hoặc viết output spatial riêng rồi merge deterministically. Full run hiện tránh bằng cách core DQ ghi đè `10/11` trước.

### C-05 — Core DQ ghi đè kết quả spatial/enrichment

- **Bằng chứng:** `run_data_quality_checks.py:86–88` ghi mới `10`, `11`; spatial và enrichment chạy sau để append/replace.
- **Ảnh hưởng:** Medium; chạy core DQ riêng làm mất spatial rule khỏi final summary.
- **Đề xuất:** Giữ run order chặt hoặc tách core/spatial/fee thành các output riêng rồi generator union.

### C-06 — Hai repair script hiện trùng logic core

- **Bằng chứng:** Exact/composite duplicate đã có ở core dòng 80–84 nhưng vẫn có `recompute_duplicate_counts.py`. Fee rule 008–010 đã có core dòng 68–70 nhưng vẫn có `enrich_temporal_fee_checks.py`.
- **Ảnh hưởng:** Low/Medium: tăng thời gian và tạo hai nguồn logic cần đồng bộ.
- **Đề xuất:** Chọn một implementation chính; giữ helper như audit script có tên/README rõ, hoặc bỏ khỏi standard run sau khi kiểm chứng.

## 6. Báo cáo và CSV có mâu thuẫn số liệu không?

Không phát hiện mâu thuẫn ở các headline đã kiểm: 204/12 link slots, 60.108.247 dòng, 7 file, 265 CSV rows, 263 features, 0 invalid geometry, file selection, exact duplicate counts và spatial success rate.

Hai điểm diễn giải cần cẩn trọng:

### C-07 — `09` tổng hợp condition không đại diện cho mọi file

- **Bằng chứng:** `dq_rules()` sort theo rule/year rồi `groupby().first()` để lấy condition (`generate_deliverables.py:51–57`) nhưng cộng checked/failed trên tất cả file. Với P99, condition chứa threshold số cụ thể khác nhau theo file; `09` chỉ giữ threshold của file đầu tiên. Spatial rule pickup/dropoff cũng chỉ giữ condition đầu tiên.
- **Ảnh hưởng:** High nếu dùng `09` làm executable rule catalog.
- **Đề xuất:** Condition registry nên là biểu thức tham số, ví dụ `duration > file_p99`; chi tiết threshold để ở `10`.

### C-08 — Tỷ lệ trong `09` là tỷ lệ aggregate theo lượt kiểm tra

- **Bằng chứng:** `checked_rows` được cộng across file/role. Với spatial, một trip có pickup và dropoff nên có thể được kiểm hai lần.
- **Ảnh hưởng:** Medium khi đọc như “tỷ lệ trip duy nhất”.
- **Đề xuất:** Ghi rõ denominator hoặc tách role/file.

## 7. Rule names code và output

Không phát hiện rule ID output không có nguồn logic: các DQ core/spatial/fee đều truy vết được; `DQ-FILE-001` và `DQ-SCHEMA-001` được generator thêm có chủ ý (`generate_deliverables.py:58–60`).

### C-09 — STM chưa tham chiếu rule tiền mới 008–010

- **Bằng chứng:** `mapping()` gán money field vào chuỗi `DQ-AMOUNT-001..007` (`generate_deliverables.py:46`), trong khi code/output có `DQ-AMOUNT-008..010`.
- **Ảnh hưởng:** Medium: mapping bỏ sót airport/CBD/effective-date checks.
- **Đề xuất:** Mở rộng DQ rule mapping theo field và version.

### C-10 — Chưa có Ratecode out-of-domain rule

- **Bằng chứng:** Core kiểm `rate IS NULL` (`DQ-CODE-002`) nhưng không có condition rate ngoài domain; yêu cầu gốc mong kiểm domain theo version.
- **Ảnh hưởng:** Medium: rate dimension có thể nhận mã lạ mà không bị flag.
- **Đề xuất:** Xây rate domain versioned từ tài liệu lịch sử trước khi thêm rule.

## 8. Hard-code và khả năng cấu hình

Không có absolute local path trong script; đây là điểm tốt. Các hard-code nghiệp vụ/cấu hình hiện có:

- `CHECKED_AT = 2026-10-30` trong `common.py:19`;
- năm representative trong `download_representative.py:11`;
- canonical maps trong `run_data_quality_checks.py:14–22`;
- airport IDs 132/138 và effective dates trong core/enrichment;
- batch size 250.000 trong spatial;
- 16 HEAD workers trong crawler.

### C-11 — Ngày kiểm tra không tự lấy runtime

- **Ảnh hưởng:** Medium khi rerun sau ngày 2026-10-30: manifest/report vẫn ghi ngày cũ.
- **Đề xuất:** Dùng runtime date theo timezone hoặc CLI parameter; giữ ngày cũ trong output snapshot nếu cần reproducibility.

### C-12 — Không có CLI để chọn year/month

- **Bằng chứng:** README yêu cầu sửa `YEARS`/logic trong source.
- **Ảnh hưởng:** Medium cho vận hành/mở rộng.
- **Đề xuất:** Thêm `--years`, `--preferred-month`, `--manifest`, `--output-snapshot`.

## 9. Script/hàm không được sử dụng hoặc khả năng dư thừa

- `common.write_json()` không được gọi.
- `SAMPLE`/`data/sample` chưa dùng.
- `crawl_tlc_manifest.py` import `time` nhưng không dùng.
- `download_representative.py` import `argparse` nhưng không dùng.
- `generate_deliverables.py` import `defaultdict` nhưng không dùng.
- `recompute_duplicate_counts.py` và `enrich_temporal_fee_checks.py` đã được chạy (có log), nhưng logic chính hiện trùng một phần với core như C-06.

Ảnh hưởng chủ yếu Low; nên cleanup sau khi nhóm chốt pipeline, nhưng không cần sửa trước buổi trình bày.

## 10. Output không truy ngược được về script

- `project_brief_extracted.txt` không có script tạo trong project.
- PDF đề bài và hai reference files là input, không phải output.
- Mọi output profile/anomaly/mapping/report còn lại đều truy được về code.

### C-13 — Yellow Data Dictionary không có bản local

- **Bằng chứng:** report tham chiếu URL TLC; `data/reference` không có dictionary PDF; README cũng không yêu cầu file này.
- **Ảnh hưởng:** Medium cho audit/offline reproducibility.
- **Đề xuất:** Lưu bản chính thức có access date/checksum nếu quy định cho phép, hoặc lưu source inventory URL + retrieved metadata.

## 11. Các giới hạn kỹ thuật khác

### C-14 — Shapefile “required components” chỉ bắt buộc 3 file

- **Bằng chứng:** `profile_shapefile.py:21` required chỉ `.shp,.shx,.dbf`; `.prj/.cpg` chỉ được liệt kê.
- **Ảnh hưởng:** Medium vì thiếu `.prj` làm CRS không xác định; thiếu `.cpg` có thể lỗi encoding.
- **Đề xuất:** Tách “minimum readable” và “project-required” gồm đủ 5 component.

### C-15 — Swapped-like coordinates vẫn nằm trong tập valid

- **Bằng chứng:** spatial code đếm `swapped`, nhưng valid chỉ loại null/zero/out-of-range (`profile_legacy_spatial.py:21–22`).
- **Ảnh hưởng:** Low/Medium; các điểm nghi đảo tiếp tục được spatial query và có thể unmatched.
- **Đề xuất:** Giữ flag riêng và báo kết quả trước/sau swap thử nghiệm, không tự sửa.

### C-16 — `download_representative` không xử lý năm không có candidate

- **Bằng chứng:** `cand.iloc[0]` tại dòng 33.
- **Ảnh hưởng:** Medium khi thêm năm/kỳ mới.
- **Đề xuất:** Raise lỗi có thông điệp rõ, không chọn ngầm.

### C-17 — Checksum được tạo nhưng không có built-in verification command

- **Bằng chứng:** code ghi hash, không có script compare hash cũ/mới.
- **Ảnh hưởng:** Low; review thủ công đã xác nhận 7/7 khớp.
- **Đề xuất:** Thêm `verify_raw_integrity.py` hoặc test tự động.

## 12. Kết luận review

Project hiện đủ evidence cho giai đoạn khảo sát và các số liệu chính nhất quán với raw. Rủi ro quan trọng nhất trước khi mở rộng là: spatial output chưa ở mức per-trip, spatial rerun không idempotent, `09` làm mất condition tham số theo file, và standard run có repair logic trùng core. Các điểm này không làm sai snapshot đầu ra hiện tại nhưng cần xử lý trước automation production.
