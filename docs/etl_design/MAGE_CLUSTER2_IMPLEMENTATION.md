# Cụm 2 Mage: giảm overhead và COPY trực tiếp

> Cập nhật 2026-11-27: nội dung transaction/direct của cụm 2 vẫn có hiệu lực, nhưng physical writer hiện là `slim-v2`. `load_stage` không còn COPY hai bản Raw và Canonical; nó thực hiện một COPY liên tục vào `stage.yellow_taxi_trip_data`, sau đó sinh technical quarantine server-side. Xem [MAGE_SLIM_V2_IMPLEMENTATION.md](MAGE_SLIM_V2_IMPLEMENTATION.md) cho kiến trúc đang chạy.

Phạm vi M02–M03, kế thừa contract và các kiểm tra M00–M01. Không sửa DDS/OLAP, công thức doanh thu, source key, schema registry hoặc policy DQ. Không migrate/nạp lại staging live trong lượt triển khai này.

## Hai đường nạp

**Upsert** tiếp tục là mặc định để giữ tương thích rollout. Một `StageWriter` dùng một engine/connection và cache schema một lần mỗi bảng trong cả block. Raw/Canonical giữ transaction theo part như trước; quarantine có session riêng theo block. Giữ kiểm duplicate/conflict trước INSERT. CSV dùng Arrow, có kiểm thử round-trip NULL, empty string, literal backslash-N, quote/newline và Unicode; pandas formatter vẫn dùng được để đối chiếu. DataFrame dùng shallow copies chỉ để thay series, không mutate input. Context/schema cache không chia qua process/thread.

**Direct** áp dụng cho đúng một file/version mỗi invocation, staging sạch. Backfill vẫn phải xử lý đủ 24 file 2024–2025 bằng vòng lặp tháng; không thu hẹp phạm vi dữ liệu đồ án. Trong `slim-v2`, `write_technical_quarantine` chỉ defer; `load_stage` COPY unified trip rows một lần và derive Technical từ `technical_valid=false`. Raw/Canonical/Technical cùng trạng thái file/batch STAGED vẫn commit trong một transaction. PK/unique được dựng lại trước publish; không tắt durability hoặc đổi persistent DQ thành UNLOGGED.

## Bật chế độ

Trong variables của pipeline Mage:

```yaml
stage_load_mode: direct
source_batch_size: 50000
canonical_batch_size: 50000
```

Metadata pipeline hiện đặt `stage_load_mode: direct`, `prepare_mode: streaming`, `stage_storage_mode: slim`. Không nạp đè batch đang STAGED: phải hoàn thành DDS/cleanup hoặc reset benchmark có chủ đích. Khi utility imports bị cache, restart dịch vụ có kiểm soát; không reset Docker volume.

Runner `run_24_month_backfill.py` nhận `MAGE_STAGE_LOAD_MODE=direct` và `MAGE_BATCH_SIZE=50000` từ môi trường của chính runner. Mặc định upsert/50k. Hai giá trị được truyền nhất quán vào source/canonical và hai exporters; không còn hardcode bên trong lời gọi từng block. 50k được giữ theo kết quả sample vì 100k gần như cùng thời gian nhưng RSS cao hơn; full-month 100k là phép đo candidate, không xác nhận tốc độ final 50k. Đây chỉ là integration Mage: flags archive, gate OLAP và lịch end-to-end của runner vẫn thuộc T08, chưa được tuyên bố đã hoàn tất.

Không có fallback tự động direct → upsert. Unknown mode FAIL; context đã defer quarantine không được chuyển sang upsert. Multi-file direct bị từ chối rõ ràng. Khi filter trả no-op, đường không ghi dữ liệu dùng hành vi cũ, không giả một direct receipt cho scope rỗng.

## Transaction và retry direct

1. Lấy session advisory lock `yellow_taxi_staging_writer`; QA dùng khóa riêng theo schema. Giữ khóa tới cuối invocation.
2. Đọc batch/step và validated file từ control, đối chiếu file/version/SHA/schema/month. SUCCESS/DBT_RUNNING không được nạp lại. Chỉ RUNNING/PENDING hoặc failed staging đủ điều kiện mới được bắt đầu.
3. Với live `slim-v2`, transaction lấy `ACCESS EXCLUSIVE NOWAIT` trên `yellow_taxi_trip_data`, `stage_file_batch` và technical quarantine. Writer khác bị chặn; dbt chỉ đọc sau STAGED.
4. Unified trip table và mapping phải rỗng; technical reject không được có rows cùng batch/version/SHA. Nếu bẩn thì FAIL, không truncate/delete/fallback.
5. Start/resume STAGING và heartbeat qua connection riêng; không giữ row lock trên batch trong lúc cần heartbeat. Code lỗi ghi trạng thái FAILED sau khi transaction data rollback.
6. Đọc từng part có giới hạn và truyền qua một COPY liên tục vào unified table; technical reject được derive server-side. Không temporary → INSERT và không ON CONFLICT. Duplicate/cast/CHECK failure hoặc quarantine mismatch rollback cả lượt.
7. Full footer/count/key partition/lineage validation trong cùng transaction, nhìn được rows của writer. Sai metrics hoặc bất kỳ kiểm tra nào FAIL thì rollback.
8. Ghi file STAGED, step SUCCESS, counters và batch STAGED trong cùng transaction. Receipt trong `etl_step_run.details` gồm `loader_mode=direct`, manifest signature và counts. Không gọi commit watermark.
9. Block verify tiếp theo kiểm DB receipt và actual Raw/Canonical/Reject counts qua compatibility views. Nó dùng chứng nhận full physical validation đã commit, không quét/sort toàn bộ keys lần thứ hai. Full audit DDS vẫn kiểm keys theo contract.

Retry sau lỗi trước commit: mọi rows/direct file status changes đã rollback, file vẫn VALIDATED; STAGING FAILED và batch FAILED với STAGING_FAILED có thể resume bằng function sẵn có. Không giả lập success.

Mất phản hồi sau commit: lần sau đọc receipt cùng manifest signature, kiểm actual data và trả lại trạng thái đã công bố; không COPY lần hai. Mismatch receipt phải FAIL, không tự nhận rows thuộc context mới.

Process bị kill khi chưa kịp ghi FAILED: PostgreSQL rollback transaction nhưng control có thể còn RUNNING. Direct từ chối khởi động writer khác; cần quy trình stale-worker/recovery được kiểm ở M06, không đổi tay sang SUCCESS. Mất kết nối trong lúc commit phải xác minh receipt; không thể xác minh thì giữ lỗi và yêu cầu retry/recovery, không nạp đè.

UNLOGGED loss sau crash: receipt còn nhưng unified trip rows mất thì actual count check FAIL; không coi receipt là bằng chứng data còn. Dữ liệu quarantine persistent không được append lại; khôi phục staging cần policy M06. Lỗi verify sau publish không gọi finish_step RUNNING lần nữa, chỉ fail batch, giữ dữ liệu để điều tra.

## Telemetry và benchmark

- `MAGE_WRITE` direct trước commit có `committed=false`; đó là tentative counters. Chỉ return từ coordinator và receipt sau commit mới xác nhận thành công. Upsert counters đã commit theo part.
- Timings COPY direct bao gồm kiểm constraints, indexes và FK của bảng đích. Không so riêng COPY temporary với COPY đích rồi kết luận direct chậm hơn.
- Harness `tests/profile_mage_m23.py` tạo toàn bộ schemas control/stage/dq/audit QA riêng từ DDL thật, có FK và functions thật. Không dùng control/staging live. Script tạo input re-batched bounded trong thư mục tạm riêng, rồi đo load + validation/receipt. Không tính chuẩn bị part, source transformation, Mage scheduler hoặc truy vấn amount parity cuối vào elapsed.
- Chạy lần lượt 50k/100k/250k cùng input để so thời gian và RSS; sample chỉ chọn cấu hình, không chứng minh tháng thật. Phép đo full-month giữ đầy đủ counts/key validation và đối soát fare/tip/total.
- SQL fixtures trong container lấy qua `MAGE_QA_SQL_ROOT`, gồm control 001/003 và stage 001/002. Không mount/đọc `.env` vào logs. Chạy tests với `MAGE_POSTGRES_QA=1`, project/tests trong PYTHONPATH. Schema QA được drop bằng prefix+UUID, không xóa schema khác.

## Bàn giao và giới hạn

Kết quả thực đo ở `reports/mage_optimization/M02.md`, `M03.md` và logs tương ứng. Nếu chưa đạt 2–4 phút thì ghi chưa đạt; không bỏ kiểm tra hoặc khai báo toàn dự án DONE. M04–M05 mới xử lý intermediate I/O, scheduler, độ rộng dữ liệu và indexes có evidence; M06–M07 nghiệm thu schedule/recovery/end-to-end thật.

Rollback code dùng snapshot M02_input_hashes và review diff, giữ sửa đổi M01 và sửa đổi trước đó của người dùng. Chuyển lại upsert chỉ cho lần chạy/scope đủ điều kiện; không dùng đổi mode để vượt dirty-target hoặc repair batch dở dang. Không cần migration mới để lưu receipt JSON; migration Raw payload 008 của cụm 1 vẫn chưa áp dụng live.
