# Evidence — Task B Docker, PostgreSQL/PostGIS và Mage

**Ngày kiểm tra:** 2026-11-16 (Asia/Bangkok)

## Cấu hình đã kiểm tra

| Thành phần | Kết quả |
|---|---|
| Docker Compose | Parse thành công |
| PostgreSQL image | `postgis/postgis:16-3.5` |
| PostgreSQL container | Healthy |
| Host endpoint | `localhost:5433` |
| Container endpoint | `postgres:5432` |
| PostGIS | `3.5 USE_GEOS=1 USE_PROJ=1 USE_STATS=1` |
| Mage endpoint | `http://localhost:6789` |
| Mage HTTP check | `200` |
| Mage → PostgreSQL | Thành công |
| Host/dbt-style → PostgreSQL | Thành công |

## Schemas

Init script tạo lại thành công đủ năm schema:

```text
control
dds
dq
olap
stage
```

## Persistence test

1. Tạo `control.persistence_smoke_test` và dòng `id=1`.
2. `docker compose restart postgres` → PostgreSQL trở lại `healthy`; dòng vẫn tồn tại.
3. `docker compose down` rồi `docker compose up -d` → dòng vẫn tồn tại.
4. Mage kết nối lại thành công sau cả hai lần khởi động.

Kết luận: named volume giữ dữ liệu khi restart hoặc down/up thông thường.

## Clean-start test

Trước khi reset, database chỉ có bảng smoke test và chưa có dữ liệu nghiệp vụ. Đã xác nhận đúng target volume `nyc_taxi_bi_postgres_data`, sau đó chạy `docker compose down --volumes` và `docker compose up -d --build`.

Kết quả:

- volume cũ bị xóa và volume rỗng mới được tạo;
- smoke table cũ không còn;
- PostGIS và năm schema được init scripts tạo lại;
- PostgreSQL trở lại trạng thái healthy;
- Mage kết nối lại PostgreSQL;
- pipeline `pl_database_connection_test` vẫn tồn tại nhờ bind mount từ host.

## Mage pipeline test

Block `test_postgres_connection` được chạy lại từ project đã bind mount và trả đủ:

```text
control
dds
dq
olap
stage
```

Mage UI tại `http://localhost:6789` trả HTTP 200.

## Dependency compatibility

Requirements của project giữ `SQLAlchemy<2.0`, không nâng `requests`, `pandas` hoặc `pyarrow` vượt giới hạn của Mage 0.9.76. `pip check` trong custom image và image gốc `mageai/mageai:0.9.76` trả cùng bốn cảnh báo dependency có sẵn từ upstream; project không bổ sung xung đột mới. Mage startup, HTTP check, GIS imports và PostgreSQL connection vẫn thành công.

Các GIS packages đã import thành công: GeoPandas 0.14.4, Pyogrio 0.10.0, Shapely 2.1.2 và GeoAlchemy2 0.14.7. Transaction smoke test cũng tạo được bảng tạm trong schema `stage` rồi rollback; log gần nhất của PostgreSQL và Mage không có `ERROR`, `FATAL`, `PANIC` hoặc traceback.

## Mage project path correction

Compose dùng `USER_CODE_PATH=/home/src/mage_project` và mount:

```text
./mage_project:/home/src/mage_project/nyc_taxi_bi
```

Điều này bảo đảm Mage đọc đúng `io_config.yaml` sử dụng environment variables thay vì file mẫu có host `hostname`.

## Việc còn chờ bên ngoài

dbt Owner cần pull commit mới, tạo `profiles.yml` cục bộ và xác nhận `dbt debug` qua `localhost:5433`. Đây là xác nhận liên thành viên; hạ tầng và host endpoint đã được kiểm tra thành công trên máy Project Lead.
