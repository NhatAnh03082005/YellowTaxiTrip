# Contract Mage staging hiện tại

Ngày cập nhật: 2026-11-28. Contract live: `slim-v2`.

## Sau khi tạo lại database

`docker compose up -d postgres mage` khởi động dịch vụ; hai script trong `docker/postgres/init` chỉ tạo extensions, schemas và grants. Database vừa xóa volume chưa có bảng control/staging hoặc migration slim, nên mở được UI không có nghĩa là chạy được ingestion.

Với database trống, triển khai SQL theo thứ tự: control tables/indexes → stage tables/indexes/comments → control functions/seed watermarks → migrations006/007 →008 minimal DDS metadata →008 raw cast payload →009 slim →010 unified slim-v2. DDL base hiện đã chứa các đối tượng clean-build của005;005 là đường nâng cấp database cũ. Không áp lại009/010 lên database đã cài hoặc đang có batch/dữ liệu staging; giữ nguyên guards của migration.

Sau khởi tạo, kiểm marker `stage.stage_storage_contract.storage_mode='slim-v2'`, watermark0 và không có batch active. Chạy `pl_database_connection_test`, sau đó `pl_reference_taxi_zone` (265lookup/263geometry), rồi mới chạy trips theo tháng đã chọn. Reference không tạo batch trips hoặc đẩy watermark.

Các block Python phải giữ decorator do Mage executor đưa vào globals. Chỉ import decorator fallback khi tên chưa tồn tại; import ghi đè sẽ gây lỗi “does not have any decorated functions” dù code có `@data_loader`/`@transformer`. Regression runtime cho mọi block trong các pipeline hiện có:

```powershell
docker compose exec -T mage python -m unittest nyc_taxi_bi.tests.integration.test_mage_block_registration -v
```

## Phạm vi

Mage sở hữu discovery, checksum/schema validation, source identity, canonicalization kỹ thuật, technical quarantine và publication đến trạng thái STAGED. Mage ingestion không tự chạy dbt/DDS, không commit watermark và không refresh OLAP.

## Identity và lineage

- Mỗi invocation direct xử lý đúng một file/version.
- `source_row_number` bắt đầu từ 1 và liên tục trong file.
- `source_record_key = SHA256(lower(file_sha256) + ':' + source_row_number)`.
- Lineage logic luôn có batch, file version, source file/month/SHA, schema fingerprint/version, row number, source key, source last modified và ingestion time.
- Metadata file bất biến nằm trong `control.source_file_version`; attempt mapping nằm trong `stage.stage_file_batch`.

## Physical và logical staging

Physical table duy nhất của trip là:

```text
stage.yellow_taxi_trip_data
```

Nó chứa Raw projection, Canonical projection, `cast_error_flags`, `raw_payload`, `technical_valid`, file version và source identity. Mỗi source row chỉ được COPY một lần.

Logical contracts:

- `stage.yellow_taxi_raw`: mọi physical row, giữ tên/type cột Raw cũ;
- `stage.yellow_taxi_canonical`: chỉ `technical_valid=true`, giữ tên/type cột Canonical cũ;
- `dq.technical_quarantine`: phần `technical_valid=false` với diagnostics và original payload.

Hai bảng `yellow_taxi_raw_data` và `yellow_taxi_canonical_data` của `slim-v1` chỉ được giữ rỗng làm rollback target; không phải writer hiện tại.

## Type và giá trị lỗi

- Canonical integer phải hữu hạn, nguyên và nằm trong SMALLINT.
- Numeric phải hữu hạn và vừa type đích; không clamp/truncate để vượt DDL.
- Invalid required fields tạo technical reject.
- Invalid optional fields thành NULL + error flag; original value nằm trong `raw_payload`.
- Business anomalies như số âm, speed hoặc passenger bất thường không tự trở thành technical reject; dbt phân loại theo policy nghiệp vụ.
- Source field/schema mới phải có registry/mapping được review; unknown column fail-closed.

## Publication nguyên tử

1. Kiểm control file/version/SHA/schema/month và target sạch.
2. Lấy advisory lock + table lock `NOWAIT`.
3. Tạm drop PK/unique của unified table trong transaction.
4. Đọc/canonicalize bounded chunks và truyền vào một PostgreSQL COPY liên tục.
5. Sinh technical quarantine server-side.
6. Đối soát footer, count, source identity, scope và Canonical/Technical partition.
7. Dựng lại PK + unique index.
8. Ghi file/step counters, receipt và chuyển batch STAGED.
9. Commit một lần; bất kỳ lỗi nào rollback toàn bộ data/control/index DDL.

Không tắt `fsync` hoặc `synchronous_commit`. Session tuning chỉ áp dụng cục bộ trong transaction loader.

## Điều kiện STAGED

Một file chỉ được STAGED khi:

1. Footer = source count = physical Raw count.
2. Raw = Canonical + Technical.
3. Mỗi source key xuất hiện đúng một physical row và đúng một nhánh Canonical/Technical.
4. Row number đủ phạm vi 1..footer và source key tái tính đúng.
5. Batch/version/file/SHA/schema/month khớp control metadata.
6. PK/unique index đã dựng lại thành công.
7. Receipt, counters và data cùng commit.

## Chế độ và vận hành

Biến đang dùng:

```yaml
prepare_mode: streaming
stage_load_mode: direct
stage_storage_mode: slim
source_batch_size: 50000
canonical_batch_size: 50000
```

`stage_storage_mode=slim` yêu cầu marker database là `slim-v1` hoặc `slim-v2`; live hiện là `slim-v2`. Không truyền literal `slim-v2` vào Mage UI.

Sau một lượt Mage thành công, batch còn STAGED. Muốn chạy tháng tiếp theo phải hoàn thành dbt/DDS + tests + reconciliation + watermark + cleanup, hoặc reset benchmark có chủ đích. Không dùng `docker compose down -v` để reset.

## Recovery và giới hạn

- Dirty target, multi-file direct, batch đang STAGED/DBT/SUCCESS hoặc receipt mismatch đều fail-closed.
- Lost acknowledgement dùng receipt + actual counts để nhận lại kết quả, không COPY lần hai.
- UNLOGGED loss sau crash phải được phát hiện bằng actual-count check.
- Migration/rollback chỉ chạy khi staging rỗng và không có batch active.
- Full 24-month schedule và Source → DDS SLA chưa được chứng minh bởi contract này.

Chi tiết implementation và benchmark: [MAGE_SLIM_V2_IMPLEMENTATION.md](MAGE_SLIM_V2_IMPLEMENTATION.md).
