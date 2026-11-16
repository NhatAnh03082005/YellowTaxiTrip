# Runbook chi tiết mục 4.1 và 4.2

**Phạm vi:** Project Lead/Integration — NYC Yellow Taxi BI 2024–2025  
**Môi trường lệnh:** Windows PowerShell  
**Thư mục project:** `D:\BI\DATH\NYC_TAXI_BI_PROJECT`

Runbook này hướng dẫn thao tác từ trạng thái hiện tại. Không đánh dấu một bước hoàn thành chỉ vì đã tạo file; phải chạy kiểm tra ở cuối bước đó.

## 0. Điều kiện trước khi bắt đầu

Design Freeze đã được Project Lead và dbt Owner ký duyệt ngày 2026-11-16. Ba file sau là baseline chính thức:

- `docs/decisions/PHASE2_DESIGN_FREEZE_PROPOSAL.md`;
- `docs/mapping/INPUT_FILES_2024_2025.md`;
- `docs/mapping/DBT_HANDOVER_CONTRACT.md`.

Không tự ý đổi contract dữ liệu trong lúc làm 4.1–4.2. Nếu phát hiện vấn đề cần thay đổi, tạo Decision Log mới và xin cả hai owner duyệt.

---

# A. Mục 4.1 — Quản lý project và dữ liệu

## A1. Xác nhận chỉ có một project gốc

1. Mở PowerShell.
2. Chuyển đến project:

```powershell
Set-Location 'D:\BI\DATH\NYC_TAXI_BI_PROJECT'
```

3. Kiểm tra các thư mục chính:

```powershell
$requiredDirectories = @(
  'data', 'dbt_project', 'deployment', 'docker', 'docs', 'evidence',
  'mage_project', 'olap', 'powerbi', 'profiling', 'reports', 'sql', 'tests'
)
$requiredDirectories | ForEach-Object {
  [pscustomobject]@{
    Directory = $_
    Exists = Test-Path -LiteralPath $_ -PathType Container
  }
} | Format-Table -AutoSize
```

4. Kết quả đạt: mọi dòng đều có `Exists = True`.
5. Không tạo project Mage, dbt hoặc PostgreSQL ở ngoài thư mục này.

### Ghi nhận trạng thái hiện tại

Cấu trúc repository thống nhất đã có. Tuy nhiên, thư mục hiện chưa phải Git repository. Nếu nhóm dùng Git, thực hiện A2; nếu repository thật nằm ở nơi khác, phải clone/copy đúng cách thay vì tạo hai repository độc lập.

## A2. Khởi tạo Git hoặc kết nối repository chung

1. Kiểm tra Git root:

```powershell
git rev-parse --show-toplevel
```

2. Nếu lệnh báo không phải Git repository và đây đúng là thư mục nhóm chọn làm repository gốc, khởi tạo:

```powershell
git init
git branch -M main
git status
```

3. Nếu nhóm đã có remote, thêm đúng URL do nhóm cung cấp:

```powershell
git remote add origin <URL_REPOSITORY_CUA_NHOM>
git remote -v
```

4. Không tạo remote giả và không đưa password/token vào URL.
5. Kết quả đạt: `git rev-parse --show-toplevel` trả đúng đường dẫn project và cả hai thành viên dùng cùng remote.

## A3. Kiểm tra tài liệu giai đoạn 1 đã đặt đúng chỗ

1. Kiểm tra báo cáo và profile:

```powershell
Test-Path -LiteralPath '.\profiling\outputs\reports\NYC_Yellow_Taxi_Source_Survey_and_Data_Quality_Report.md'
Test-Path -LiteralPath '.\profiling\outputs\profiles\01_yellow_taxi_file_manifest.csv'
Test-Path -LiteralPath '.\docs\data_survey\handover\01_PROJECT_OVERVIEW.md'
Test-Path -LiteralPath '.\evidence\run_logs\phase1_survey'
```

2. Kết quả đạt: cả bốn lệnh trả `True`.
3. Trạng thái hiện tại: phần này đã được tổ chức đúng; không cần di chuyển lại.

## A4. Tạo `.env.example`, nhưng không tạo secret trong file mẫu

1. Tạo file `.env.example` ở project root với các tên biến sau:

```dotenv
COMPOSE_PROJECT_NAME=nyc_taxi_bi
POSTGRES_DB=nyc_taxi
POSTGRES_USER=nyc_taxi
POSTGRES_PASSWORD=replace_with_local_password
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_HOST_PORT=5432
MAGE_PORT=6789
MAGE_PROJECT_NAME=nyc_taxi_bi
```

2. Sao chép thành `.env` để dùng cục bộ:

```powershell
Copy-Item -LiteralPath '.env.example' -Destination '.env'
```

3. Mở `.env`, thay `POSTGRES_PASSWORD` bằng password cục bộ mạnh.
4. Không sửa password thật vào `.env.example`.
5. Xác nhận `.env` bị Git bỏ qua:

```powershell
git check-ignore -v .env
```

6. Kết quả đạt: lệnh chỉ ra rule `.env` trong `.gitignore`.

## A5. Rà soát `.gitignore`

File hiện tại đã bỏ qua `.env`, `profiles.yml`, dữ liệu trip lớn, dbt artifacts và cache Power BI. Kiểm tra thêm:

```powershell
git check-ignore -v .env
git check-ignore -v data/yellow_taxi/by_year_month/test.parquet
git check-ignore -v dbt_project/target/manifest.json
git check-ignore -v dbt_project/profiles.yml
```

Nếu nhóm giải nén Shapefile nhỏ vào `data/reference/taxi_zones/`, không ignore thư mục đó vì đây là dữ liệu tham chiếu cần chia sẻ.

Trước mỗi commit, chạy:

```powershell
git status --short
git diff --cached
```

Không commit `.env`, password, token, volume PostgreSQL, file Parquet nguồn lớn hoặc file tạm Power BI.

## A6. Hoàn thiện catalog 24 file ETL

1. Dùng `docs/mapping/INPUT_FILES_2024_2025.md` làm danh sách phạm vi đã duyệt.
2. Tạo thư mục catalog nếu chưa có:

```powershell
New-Item -ItemType Directory -Path '.\data\catalog' -Force
```

3. Tạo `data/catalog/yellow_taxi_2024_2025.csv` với đúng một dòng cho mỗi file và các cột:

```text
source_year_month,file_name,source_url,http_status,content_length,last_modified,sha256,schema_fingerprint,schema_version,local_path,downloaded_at,validation_status
```

4. Sinh đủ `2024-01` đến `2024-12` và `2025-01` đến `2025-12`.
5. Trước khi tải, `sha256`, `schema_fingerprint` và `downloaded_at` được để trống; `validation_status` đặt là `DISCOVERED`.
6. Kiểm tra số dòng dữ liệu:

```powershell
$catalog = Import-Csv '.\data\catalog\yellow_taxi_2024_2025.csv'
$catalog.Count
$catalog.source_year_month | Sort-Object -Unique
```

7. Kết quả đạt: có đúng 24 dòng và 24 giá trị `source_year_month` duy nhất.

## A7. Tải từng file vào đúng partition

Không tải đồng thời cả 24 file trong lần thử đầu tiên. Làm một file đại diện của 2024 và một file của 2025 trước.

1. Tạo thư mục cho tháng:

```powershell
New-Item -ItemType Directory -Path '.\data\yellow_taxi\by_year_month\2024-05' -Force
```

2. Tải về file tạm:

```powershell
$url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet'
$temporaryFile = '.\data\yellow_taxi\by_year_month\2024-05\yellow_tripdata_2024-05.parquet.tmp'
$finalFile = '.\data\yellow_taxi\by_year_month\2024-05\yellow_tripdata_2024-05.parquet'
Invoke-WebRequest -Uri $url -OutFile $temporaryFile
```

3. Kiểm tra file không rỗng:

```powershell
Get-Item -LiteralPath $temporaryFile | Select-Object FullName, Length
```

4. Chỉ đổi sang tên chính thức sau khi kiểm tra Parquet footer thành công ở A8:

```powershell
Move-Item -LiteralPath $temporaryFile -Destination $finalFile
```

5. Lặp lại cho 2025-05, sau đó mới thực hiện 22 file còn lại.
6. Không commit các file này; `.gitignore` phải loại chúng.

## A8. Kiểm tra Parquet và tính SHA-256

1. Tạo virtual environment cho công cụ kiểm tra nếu chưa có:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r .\profiling\requirements.txt
```

2. Kiểm tra metadata/footer bằng PyArrow:

```powershell
python -c "import pyarrow.parquet as pq; p=r'data/yellow_taxi/by_year_month/2024-05/yellow_tripdata_2024-05.parquet.tmp'; f=pq.ParquetFile(p); print(f.metadata.num_rows); print(f.schema_arrow)"
```

3. Nếu lệnh lỗi, giữ file `.tmp`, ghi lỗi vào evidence và không đổi sang tên chính thức.
4. Nếu hợp lệ, đổi tên như A7.4 rồi tính hash:

```powershell
Get-FileHash -Algorithm SHA256 -LiteralPath $finalFile
```

5. Ghi `content_length`, `sha256`, schema, thời điểm tải và `validation_status=VALIDATED` vào catalog.
6. Chạy `Get-FileHash` lần hai; kết quả phải giống lần đầu.
7. Kết quả đạt cho toàn bộ phạm vi: 24 file có SHA-256 dài 64 ký tự, footer đọc được và không có `validation_status` khác `VALIDATED`.

## A9. Chuẩn bị Taxi Zone CSV và Shapefile

1. CSV đã có tại `data/reference/taxi+_zone_lookup.csv`.
2. ZIP đã có tại `data/reference/taxi_zones.zip`.
3. Giải nén vào thư mục cố định:

```powershell
New-Item -ItemType Directory -Path '.\data\reference\taxi_zones' -Force
Expand-Archive -LiteralPath '.\data\reference\taxi_zones.zip' -DestinationPath '.\data\reference\taxi_zones' -Force
Get-ChildItem -LiteralPath '.\data\reference\taxi_zones'
```

4. Kiểm tra bộ Shapefile có tối thiểu `.shp`, `.shx`, `.dbf`, `.prj`.
5. Kiểm tra lookup có 265 LocationID duy nhất:

```powershell
$zones = Import-Csv '.\data\reference\taxi+_zone_lookup.csv'
$zones.Count
($zones.LocationID | Sort-Object -Unique).Count
```

6. Kết quả mong đợi: 265 dòng/ID trong CSV; Shapefile có 263 geometry. Zone 264 và 265 không có polygon là trường hợp đã biết, không phải lỗi tải file.

## A10. Tạo sample nhỏ cho unit/integration test

1. Tạo hai nhóm fixture:
   - `tests/fixtures/valid/`: dữ liệu hợp lệ, nhỏ, chạy nhanh;
   - `tests/fixtures/invalid/`: các lỗi có chủ đích như cast lỗi, datetime đảo, amount âm và duplicate.
2. Tạo sample Parquet đại diện từ cả hai schema 2024 và 2025; dùng seed cố định để lần sau tạo lại cùng kết quả.
3. Sample không được chỉ lấy các dòng đầu nếu cần kiểm tra DQ. Nên gồm:
   - các dòng bình thường;
   - null/unknown code;
   - Zone 264/265;
   - một số dòng cảnh báo/quarantine đã biết;
   - cột `cbd_congestion_fee` của schema 2025.
4. Mỗi sample nên đủ nhỏ để commit được, ví dụ vài nghìn dòng và không chứa dữ liệu nhạy cảm bổ sung.
5. Ghi `tests/fixtures/README.md` nêu file nguồn, SHA-256 nguồn, seed, cách chọn dòng và expected row count.
6. Kết quả đạt: thành viên dbt clone repository có thể chạy test chỉ bằng fixture, không cần file nguồn lớn trên máy bạn.

## A11. Loại bỏ phụ thuộc đường dẫn máy cá nhân

1. Tìm đường dẫn tuyệt đối bị hard-code:

```powershell
rg -n --hidden --glob '!data/yellow_taxi/**' --glob '!.git/**' '([A-Za-z]:\\|/Users/|/home/)' .
```

2. Thay đường dẫn thực thi bằng biến môi trường hoặc đường dẫn tương đối từ project root.
3. Trong tài liệu có thể ghi đường dẫn ví dụ, nhưng code/pipeline không được phụ thuộc `D:\BI\DATH` hoặc `C:\Users\...`.
4. Nhờ thành viên dbt clone vào đường dẫn khác và chạy test fixture.

## A12. Checklist hoàn thành mục 4.1

- [x] Git root/remote chung đã thống nhất.
- [x] `.env.example` không chứa secret; `.env` bị ignore.
- [x] Catalog có đúng 24 file và metadata bắt buộc.
- [x] 24 Parquet đọc được footer và có SHA-256.
- [x] CSV và đầy đủ thành phần Shapefile nằm trong `data/reference/`.
- [x] Fixture 2024/2025 và invalid fixture chạy độc lập với máy Project Lead.
- [x] Không có password hoặc đường dẫn máy cá nhân trong source thực thi.
- [x] Fresh clone từ repository chung nhận đủ sample/schema/contract và chạy `tests/validate_fixtures.py` thành công.

---

# B. Mục 4.2 — Docker và PostgreSQL/PostGIS

## B1. Kiểm tra công cụ

1. Mở Docker Desktop và đợi engine sẵn sàng.
2. Chạy:

```powershell
docker --version
docker compose version
docker info
```

3. Kết quả đạt: ba lệnh thành công. Ở máy hiện tại đã phát hiện Docker 29.6.2 và Docker Compose 5.3.1, nhưng vẫn phải kiểm tra `docker info` sau khi Docker Desktop chạy.

## B2. Chốt bố cục file Docker

Tạo/duy trì đúng các vị trí sau:

```text
docker-compose.yml
.env.example
docker/
  postgres/
    init/
      001_extensions_and_schemas.sql
      002_grants.sql
  mage/
    Dockerfile
mage_project/
```

Không lưu dữ liệu vật lý của PostgreSQL vào Git. Dùng named volume do Docker quản lý.

## B3. Tạo init script cho extension và schemas

Nội dung tối thiểu của `docker/postgres/init/001_extensions_and_schemas.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS olap;
```

Lưu ý: script trong `/docker-entrypoint-initdb.d/` chỉ tự chạy khi volume PostgreSQL được khởi tạo lần đầu. Sửa script sau khi volume đã tồn tại không tự áp dụng lại; lúc đó phải chạy migration thủ công hoặc reset volume có chủ đích.

## B4. Tạo quyền truy cập tối thiểu

Trong `002_grants.sql`, trước giai đoạn phân quyền chi tiết, cấp quyền cho user ứng dụng đã được tạo bởi image PostgreSQL:

```sql
GRANT USAGE, CREATE ON SCHEMA stage, control, dq, dds, olap TO nyc_taxi;
ALTER DEFAULT PRIVILEGES IN SCHEMA stage, control, dq, dds, olap
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO nyc_taxi;
ALTER DEFAULT PRIVILEGES IN SCHEMA stage, control, dq, dds, olap
GRANT USAGE, SELECT ON SEQUENCES TO nyc_taxi;
```

Nếu `POSTGRES_USER` khác `nyc_taxi`, không hard-code tên này; dùng init template hoặc giữ cùng một tên user đã thống nhất trong `.env.example`.

## B5. Tạo Mage image

1. Chọn một Mage image/version đã kiểm thử và pin tag cụ thể; không để `latest` trong bản nộp cuối.
2. `docker/mage/Dockerfile` cần:
   - dùng Mage làm base image;
   - cài PostgreSQL client và các Python dependency cần cho Parquet/PostGIS;
   - đặt working directory trùng mount của `mage_project`;
   - không nhúng password vào image.
3. Đặt dependency Python riêng trong `mage_project/requirements.txt` hoặc file được Dockerfile sử dụng.
4. Build thử riêng Mage:

```powershell
docker compose build mage
```

5. Kết quả đạt: build không lỗi và không in secret trong build log.

## B6. Tạo `docker-compose.yml`

Compose phải có tối thiểu:

- service `postgres` dùng image PostgreSQL có PostGIS được pin version;
- service `mage` build từ `docker/mage/Dockerfile`;
- named volume `postgres_data`;
- network `nyc_taxi_network`;
- healthcheck cho PostgreSQL;
- `mage` phụ thuộc trạng thái healthy của PostgreSQL;
- port host lấy từ `.env`;
- mount init scripts read-only;
- mount `mage_project` để phát triển pipeline.

Khung tham khảo:

```yaml
services:
  postgres:
    image: postgis/postgis:<PINNED_TAG>
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "${POSTGRES_HOST_PORT}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docker/postgres/init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 12
    networks: [nyc_taxi_network]

  mage:
    build:
      context: .
      dockerfile: docker/mage/Dockerfile
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
    ports:
      - "${MAGE_PORT}:6789"
    volumes:
      - ./mage_project:/home/src/mage_project
      - ./data:/home/src/data
    depends_on:
      postgres:
        condition: service_healthy
    networks: [nyc_taxi_network]

volumes:
  postgres_data:

networks:
  nyc_taxi_network:
    driver: bridge
```

Thay `<PINNED_TAG>` bằng tag PostGIS đã được nhóm kiểm thử. Không chạy Compose khi placeholder còn tồn tại.

## B7. Kiểm tra cấu hình trước khi khởi động

1. Đảm bảo `.env` tồn tại và đã thay password mẫu.
2. Render/validate Compose:

```powershell
docker compose config --quiet
docker compose config --services
```

3. Kết quả mong đợi: có `postgres` và `mage`; không có lỗi biến môi trường.
4. Không chia sẻ output đầy đủ của `docker compose config` nếu output làm lộ password đã interpolate.

## B8. Khởi động PostgreSQL trước

```powershell
docker compose up -d postgres
docker compose ps
docker compose logs --tail 100 postgres
```

Chờ trạng thái `healthy`. Nếu không healthy:

```powershell
docker compose logs postgres
```

Không tiếp tục sang Mage cho tới khi PostgreSQL healthy.

## B9. Kiểm tra PostGIS và năm schemas

Chạy từ container để không phụ thuộc `psql` trên Windows host:

```powershell
docker compose exec postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT PostGIS_Full_Version();"
docker compose exec postgres psql -U $env:POSTGRES_USER -d $env:POSTGRES_DB -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('stage','control','dq','dds','olap') ORDER BY schema_name;"
```

PowerShell không tự nạp `.env` vào `$env:*`. Nếu hai biến chưa có trong phiên shell, thay bằng giá trị user/database không nhạy cảm đã chọn, ví dụ `-U nyc_taxi -d nyc_taxi`; không đưa password vào command line.

Kết quả đạt:

- `PostGIS_Full_Version()` trả thông tin version;
- truy vấn schema trả đúng `control`, `dds`, `dq`, `olap`, `stage`.

## B10. Khởi động và kiểm tra Mage

```powershell
docker compose up -d mage
docker compose ps
docker compose logs --tail 100 mage
```

1. Mở `http://localhost:6789` hoặc port đã đặt trong `.env`.
2. Tạo connection test trong Mage với:
   - host: `postgres`;
   - port: `5432`;
   - database/user/password: lấy từ environment;
   - không dùng `localhost` từ bên trong container Mage.
3. Chạy query `SELECT current_database(), current_user;`.
4. Kết quả đạt: UI mở được và query trả đúng database/user.

## B11. Kiểm tra kết nối từ dbt/host

Từ Windows host, dbt dùng:

- host: `localhost`;
- port: giá trị `POSTGRES_HOST_PORT` (mặc định 5432);
- database/user/password: từ `.env` cục bộ.

Từ container trong cùng Compose network, dùng:

- host: `postgres`;
- port: `5432`.

Đây là điểm thường gây nhầm: `localhost` trong container Mage là chính container Mage, không phải PostgreSQL.

## B12. Kiểm tra persistent volume

1. Tạo bảng và một dòng kiểm tra:

```powershell
docker compose exec postgres psql -U nyc_taxi -d nyc_taxi -c "CREATE TABLE IF NOT EXISTS control.persistence_smoke_test (id integer PRIMARY KEY, note text); INSERT INTO control.persistence_smoke_test VALUES (1, 'survives restart') ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note;"
```

2. Restart service, không xóa volume:

```powershell
docker compose restart postgres
docker compose ps
```

3. Sau khi healthy, đọc lại:

```powershell
docker compose exec postgres psql -U nyc_taxi -d nyc_taxi -c "SELECT * FROM control.persistence_smoke_test;"
```

4. Kết quả đạt: dòng `id=1` vẫn còn.

## B13. Kiểm tra trên môi trường sạch

Chỉ thực hiện sau khi đã sao lưu mọi dữ liệu cần giữ.

> **Cảnh báo phá hủy dữ liệu:** `docker compose down --volumes` xóa named volume của Compose project, gồm toàn bộ database PostgreSQL trong volume đó. Chỉ chạy tại đúng thư mục project và khi đã xác nhận đây là database thử nghiệm có thể tạo lại.

1. Ghi lại project và volume sắp tác động:

```powershell
docker compose ls
docker volume ls
docker compose config --volumes
```

2. Đứng đúng tại `D:\BI\DATH\NYC_TAXI_BI_PROJECT`.
3. Khi đã xác nhận có thể xóa database thử nghiệm:

```powershell
docker compose down --volumes
docker compose up -d --build
docker compose ps
```

4. Chạy lại B9 và B10.
5. Kết quả đạt: từ volume rỗng, PostGIS và năm schemas tự tạo, PostgreSQL healthy, Mage kết nối được.

## B14. Các lệnh vận hành phải ghi trong README/runbook

Khởi động:

```powershell
docker compose up -d
```

Xem trạng thái:

```powershell
docker compose ps
```

Xem log gần nhất:

```powershell
docker compose logs --tail 200 postgres
docker compose logs --tail 200 mage
```

Theo dõi log trực tiếp; thoát bằng `Ctrl+C`:

```powershell
docker compose logs -f mage
```

Dừng container nhưng giữ dữ liệu:

```powershell
docker compose down
```

Restart một service:

```powershell
docker compose restart postgres
docker compose restart mage
```

Reset database thử nghiệm: chỉ dùng quy trình có cảnh báo ở B13, không đưa `--volumes` vào lệnh dừng thông thường.

## B15. Thu thập evidence

Lưu vào `evidence/run_logs/phase2_infrastructure/`:

- output `docker compose ps`;
- log khởi tạo PostgreSQL;
- kết quả `PostGIS_Full_Version()`;
- danh sách năm schemas;
- kết quả persistence smoke test trước và sau restart;
- ảnh Mage UI và connection test;
- kết quả clean-start test.

Không lưu password hoặc toàn bộ `.env` trong evidence.

## B16. Checklist hoàn thành mục 4.2

- [ ] `docker compose config --quiet` thành công.
- [ ] PostgreSQL/PostGIS healthy.
- [ ] Có đủ `stage`, `control`, `dq`, `dds`, `olap`.
- [ ] Mage truy cập PostgreSQL bằng hostname `postgres`.
- [ ] dbt/host truy cập PostgreSQL bằng `localhost` và host port.
- [ ] Restart không mất dữ liệu.
- [ ] Clean start từ volume rỗng tạo lại được toàn bộ infrastructure.
- [ ] Có hướng dẫn up/down/log/reset với cảnh báo rõ cho reset volume.
- [ ] Cả hai thành viên kết nối được cùng PostgreSQL.

---

# C. Thứ tự thực hiện ngắn gọn

1. A1–A5: repository, Git, `.env.example`, `.gitignore`.
2. A6–A9: catalog, tải/kiểm tra file và reference data.
3. A10–A12: fixtures, bỏ hard-code path, nghiệm thu 4.1.
4. B1–B7: tạo và validate Docker configuration.
5. B8–B12: chạy PostgreSQL, PostGIS, Mage và test persistence.
6. B13–B16: clean-start test, runbook vận hành và evidence.

Chỉ chuyển sang mục 4.3 sau khi checklist A12 và B16 đều đạt.
