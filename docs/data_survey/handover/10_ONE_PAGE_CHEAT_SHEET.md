# 10. NYC Taxi Survey — One-page Cheat Sheet

## Project làm gì?

Khảo sát nguồn và chất lượng NYC Yellow Taxi 2009–2026 trước khi thiết kế Source → Staging → DDS. Project crawl 204 link, tải 7 file đại diện và quét 60.108.247 dòng, không sửa raw.

## Ba nguồn

1. Yellow Taxi Parquet: giao dịch/chuyến.
2. Taxi Zone CSV: 265 LocationID + Borough/Zone/service zone.
3. Taxi Zone Shapefile: 263 polygon, EPSG:2263.

## Bảy file đại diện

`2009-01`, `2010-01`, `2015-01`, `2019-01`, `2024-04`, `2025-01`, `2026-01`. Năm 2024 dùng tháng 4 vì Jan–Mar không có anchor trong manifest HTML.

## Luồng

`TLC page → 01 manifest → raw/representative → 02/03 profile → 04 evolution → 05–08 zone/shape → core + spatial DQ → 09/10/11 → 12 mapping → report`

## Năm output nên mở trước

- `01_yellow_taxi_file_manifest.csv`: source coverage.
- `04_schema_evolution.csv`: schema thay đổi.
- `09_data_quality_rules.csv`: rule registry.
- `10_anomaly_summary.csv`: số lỗi theo file.
- `12_source_to_target_mapping.csv`: ETL mapping.

## Phát hiện lớn

- 2009/2010 dùng tọa độ; 2015+ dùng LocationID trong mẫu.
- `cbd_congestion_fee` xuất hiện 2025+; `Airport_fee` đổi case.
- CSV 265 ID; shape 263 feature; 264/265 chỉ CSV.
- Shape: EPSG:2263, 0 invalid/null/empty geometry.
- Spatial success khoảng 97,6–97,9% tổng dòng legacy.
- Exact duplicate dư: 31 (2009), 16 (2010), 229 (2015), 0 ở bốn file sau.
- Passenger null: 11,63% (2024), 15,54% (2025), 29,21% (2026).
- Money mismatch là cờ điều tra, không tự kết luận total sai.

## Grain và dimensions

- Grain: một bản ghi chuyến hoàn tất meter-on → meter-off.
- Fact: trip_count, passenger, distance, duration, speed, fare/tax/tip/tolls/fees/total, DQ flags.
- Dimensions: date, time, zone, vendor, payment type, rate code.
- Role-playing: pickup/dropoff date, time, zone.

## Quyết định còn mở

Threshold duration/speed; passenger 0/>6; reversal; money tolerance; Unknown key; spatial boundary; duplicate survivorship; historical domains; revenue theo pickup/dropoff; SCD strategy.

## Lệnh quan trọng

```powershell
uv venv .venv --python 3.12
uv pip install --python .venv\Scripts\python.exe -r nyc_taxi_survey\requirements.txt
.venv\Scripts\python.exe nyc_taxi_survey\scripts\crawl_tlc_manifest.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\download_representative.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\run_data_quality_checks.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_legacy_spatial.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\generate_deliverables.py
```

Để chạy đúng đủ bước, dùng `06_EXECUTION_RUNBOOK.md`.
