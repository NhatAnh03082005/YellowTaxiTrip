# Hồ sơ quyết định pilot tối ưu lưu trữ

Trạng thái đề xuất ngày 2026-11-19 là **NO-GO**. Phạm vi xem xét là một file trước, `2024-05`, sau khi các gate dưới đây đạt. Bằng chứng hiện có và kết quả chính thức nằm trong [OPTIMIZATION_PROGRESS.md](../../OPTIMIZATION_PROGRESS.md). Tiêu chí nghiệp vụ theo [đặc tả V2](../../docs/etl_design/STORAGE_OPTIMIZATION_FINAL_24_MONTHS.md).

## Bằng chứng sẵn sàng và còn thiếu

| Gate | Bằng chứng | Còn thiếu |
| --- | --- | --- |
| Backup và batch cũ | Backup custom đã restore. Batch 34 được đóng FAILED với mã `DOWNSTREAM_STAGING_LOST`. Nguồn version 5 khớp SHA-256, watermark giữ nguyên. | Duyệt batch mới restage đúng version 5 trong lần chạy được phép. |
| Replacement | QA đã kiểm Fact rollback, lỗi giữa hai model OLAP, rollback transaction watermark/reconciliation, retry cùng batch và gọi lại idempotent. Cặp version có binding bền vững và chặn batch mới khi pending. | Duyệt migration 007 và cách cô lập consumer khi batch pending. Các dbt model commit riêng lẻ, vì vậy đọc trực tiếp bảng vẫn có thể thấy dữ liệu đang chuyển trạng thái trước watermark. |
| Historical parity và DUP-002 | Mẫu 2.048 dòng thực khớp lineage và hai fingerprint. QA có 0 sai khác trên 10.129 dòng và DUP-002 cả hai version sau retire. | Legacy chỉ có một version. Bản restore thiếu ba nhánh persisted cho version 5. Cần rebuild có ngân sách và parity hai chiều đầy đủ trước cutover. |
| FIN-15 | Snapshot D và PostgreSQL ở checkpoint. Đã xác minh footer 24 file tổng 89.892.322 dòng. | Peak đo được và forecast một file có biên an toàn. Index/TOAST, WAL, temp và work chưa được định lượng tại tải thật. |

## Ngân sách và kiểm soát dừng

- D tổng 125.829.115.904 byte, trống 43.266.707.456 byte tại lần đo mới, 25% dự phòng là 31.457.278.976 byte. Có khoảng 11,00 GiB khả dụng trên mức dự phòng. Giữ thêm 2 GiB làm biên dừng đề xuất, ngân sách để duyệt workload hiện khoảng 9,00 GiB. Raw/Canonical baseline một tháng khoảng 4,97 GiB, còn khoảng 4,03 GiB cho các thành phần tăng thêm. PostgreSQL WAL directory khoảng 1 GiB ở checkpoint trước đã nằm trong dung lượng dùng, chỉ tính tăng thêm.
- Forecast host dùng đỉnh của **tổng đồng thời** theo thời gian. Theo dõi phần tăng VHDX được đo qua host và phần tăng work/backup/log ngoài VHDX. Phân rã tăng trưởng VHDX thành Staging, Fact/DQ/index/TOAST, WAL và temp để xác định nguyên nhân. Các thành phần trong VHDX đã bao gồm trong phần tăng VHDX nên chỉ cộng một lần. Đối chiếu với giảm free bytes của D.
- Dùng high-water của các phase COPY, dbt/DQ, OLAP và reconciliation để dự báo với số dòng, kích thước nguồn, phân bố disposition và số index. Giữ riêng hệ số biến động WAL/temp và dùng ước lượng cao nhất có căn cứ. Chỉ xét GO khi forecast và biên dừng nằm trên mức dự phòng, ổ chứa work/log và filesystem guest đều đủ chỗ, và storage policy cho pilot được hai owner chấp thuận. Không dùng kích thước QA cuối lượt để ngoại suy tuyến tính cho peak.
- Lấy mẫu free bytes host, kích thước/allocated VHDX, PostgreSQL relation/index/TOAST, WAL directory/LSN, temp delta và work directory mỗi 2 giây. Gắn application name duy nhất cho phiên QA, ghi phase và PID. Cảnh báo tại mức dự phòng + 4 GiB. Dừng tại mức dự phòng + 2 GiB hoặc khi chạm cap/tốc độ tăng có thể vượt biên giữa hai lần đo, tùy điều kiện nào đến trước. Ngưỡng là đề xuất và phải thử cơ chế dừng bằng workload nhỏ đã duyệt.
- Watchdog cần độc lập với runner. Khi dừng, chặn phase tiếp theo, hủy process tree của phép thử và các phiên PostgreSQL đã xác minh theo database, application name và PID. Cho transaction lỗi rollback, lưu số đo và giữ Staging để kiểm tra trạng thái. Watermark và cleanup theo trạng thái commit thực tế.

## Phép thử đo nhỏ đề xuất

- Chạy trên database QA riêng và dùng tối đa 20.000 dòng từ fixture/Parquet đã giới hạn trước. Thử một lane lần lượt với một writer, tối đa hai dbt connections, `statement_timeout=60s`, `lock_timeout=5s`, `temp_file_limit=64MB` mỗi session và deadline wall-clock 10 phút. Tách work directory QA và xác minh database/volume trước khi chạy.
- Dừng khi mức giảm free space so baseline đạt 512 MiB, phần tăng database đạt 256 MiB, hoặc work directory đạt 128 MiB. Đây là ngưỡng giám sát bên cạnh ngân sách host và giới hạn timeout. Chỉ sử dụng một fixture batch, thu thập bytes/row cho từng branch, WAL/temp/TOAST và tăng VHDX, rồi dùng kết quả để thu hẹp forecast. Một mẫu QA nhỏ chỉ hiệu chỉnh giả định, chưa đủ làm số đo peak FIN-15 của file thật.

### Trình tự thực hiện sau khi được duyệt

1. Ghi rõ ID phép thử, database QA, ổ chứa VHDX và work/log, checksum Parquet và manifest tối đa 20.000 dòng **tổng cộng**. Giới hạn đầu đọc trước khi mở transaction ghi. Dùng số dòng dự kiến và số dòng đọc/ghi thực tế để kiểm cap, gồm cả reject. Chạy tuần tự, chỉ một tiến trình điều khiển và tối đa hai dbt threads. Cấu hình timeout và xác minh `SHOW statement_timeout`, `SHOW lock_timeout`, `SHOW temp_file_limit` từ đúng phiên workload.
2. Ghi baseline 30 giây khi QA yên, gồm free bytes trên từng ổ và guest, database bytes, WAL, temp, VHDX và work. Nếu thiếu dữ liệu giám sát, phát hiện workload khác dùng cùng ngân sách, hoặc không thể gắn session/PID đúng phép thử, giữ NO-GO. Thử đường dừng của watchdog bằng tiến trình mô phỏng vô hại trước khi cấp quyền ghi.
3. Giám sát mỗi 2 giây bằng watchdog ngoài worker. Cap 512/256/128 MiB là **ngưỡng dừng quan sát**, có thể vượt giữa hai mẫu và trong lúc hủy truy vấn. Nếu không thiết lập được giới hạn dung lượng vật lý tách riêng, đo độ trễ hủy và giới hạn tốc độ ghi theo chunk trước khi chạy. Coi mất hai mẫu liên tiếp là tín hiệu dừng. Dừng sớm khi `free - tốc độ giảm × (chu kỳ lấy mẫu + độ trễ hủy)` dự kiến chạm ngưỡng, hoặc dự kiến vượt bất kỳ cap nào.
4. Sau tín hiệu dừng, khóa không cho phase mới, hủy process tree và chỉ hủy phiên QA đã khớp database, application name và PID. Nếu query chưa thoát trong 5 giây, dùng terminate đúng phiên QA. Tiếp tục lấy mẫu đến khi hết phiên ghi và free bytes ổn định. Ghi phase, giờ, lý do, peak quan sát và số dòng thực tế. Kiểm tra control state và giữ Staging để điều tra khi run chưa commit.
5. Báo cáo riêng peak thực đo trên QA và forecast một tháng theo ba thành phần gồm tăng persistent/temporary trong PostgreSQL, tăng file ngoài VHDX và biên bất định. Tính peak từng phase từ dấu thời gian chung. Dùng hệ số theo số dòng, disposition và index, ghi giả định WAL/temp riêng. So forecast upper bound với ngân sách trên từng ổ. Báo cáo NO-GO nếu chưa có upper bound, thiếu benchmark spill hoặc không xác minh được độ trễ dừng. Kết quả phép thử nhỏ chỉ là đầu vào để duyệt pilot một file ở quyết định sau.

## Phương án bổ sung historical parity

- Ưu tiên snapshot thật có cả legacy registry và các nhánh persisted cho nhiều file version, kèm catalog, checksum, version của code và evidence batch SUCCESS. Kiểm manifest phạm vi trước khi chạy. Dùng môi trường QA có dung lượng riêng, so hai chiều theo từng version trên bộ khóa, lineage, raw-exact fingerprint, candidate fingerprint và disposition. Đếm thiếu/thừa/duplicate theo nhánh, các mismatch dùng sample key được băm. Đối chiếu thêm cặp version và loại match của DUP-002 cùng tính quan sát từ cả hai đầu.
- Nếu chỉ có backup hiện tại, cần duyệt một môi trường đủ dung lượng để rebuild version 5 từ đúng Parquet bằng baseline SQL đã pin và candidate SQL độc lập. Kết quả có thể chứng minh parity của version 5. File thật 2025 có thể bổ sung kiểm chứng cross-file sau khi có baseline độc lập trên ít nhất hai version thật. Không coi kết quả đó là phục hồi lịch sử đã mất hoặc suy ra DUP-002 nếu dữ liệu không tạo cặp. Giữ kết quả mẫu 2.048 dòng hiện tại ở trạng thái sampled.
- Ước lượng query bằng metadata và EXPLAIN trước, xử lý theo phiên bản hoặc dải khóa có bounded temp, và ghi timeout. So trực tiếp legacy với candidate trong cùng snapshot read-only khi có đủ hai nhánh. Việc rebuild dữ liệu thật cần quyết định riêng ngoài phép thử 20.000 dòng.

## Quyết định owner

| Vai trò | Điểm cần xem xét | Quyết định | Người ký / ngày / bằng chứng |
| --- | --- | --- | --- |
| Project Lead / Integration | Backup, restage version 5, ngân sách host, cap, ngưỡng dừng, phạm vi một file | PENDING | |
| dbt Owner | Scope và rollback của replacement, phục hồi runner/OLAP, parity persisted, DUP-002, lineage, benchmark index | PENDING | |

Các quyết định cần ghi riêng là cho phép phép thử QA giới hạn, chọn nguồn bằng chứng historical parity, và duyệt pilot một file sau khi đã có peak forecast cùng phương án cô lập consumer lúc replacement đang pending. Phiếu cho phép đo QA không mở Task 6.
