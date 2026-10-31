from __future__ import annotations

from collections import defaultdict

import pandas as pd

from common import ANOMALIES, CHECKED_AT, MAPPINGS, PROFILES, REPORTS, TLC_PAGE, init_dirs, logger
from run_data_quality_checks import CANON

YEARS=[2009,2010,2015,2019,2024,2025,2026]
CANONICAL=["vendor_id","pickup_datetime","dropoff_datetime","passenger_count","trip_distance","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","rate_code_id","store_and_fwd_flag","pu_location_id","do_location_id","payment_type_id","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","congestion_surcharge","airport_fee","cbd_congestion_fee"]
KEYMAP={"vendor":"vendor_id","pickup":"pickup_datetime","dropoff":"dropoff_datetime","passenger":"passenger_count","distance":"trip_distance","pu_lon":"pickup_longitude","pu_lat":"pickup_latitude","do_lon":"dropoff_longitude","do_lat":"dropoff_latitude","rate":"rate_code_id","store":"store_and_fwd_flag","pu":"pu_location_id","do":"do_location_id","payment":"payment_type_id","fare":"fare_amount","extra":"extra","mta":"mta_tax","tip":"tip_amount","tolls":"tolls_amount","improvement":"improvement_surcharge","total":"total_amount","congestion":"congestion_surcharge","airport":"airport_fee","cbd":"cbd_congestion_fee"}
TYPES={"vendor_id":"VARCHAR","pickup_datetime":"TIMESTAMP","dropoff_datetime":"TIMESTAMP","passenger_count":"DECIMAL(8,2)","trip_distance":"DOUBLE","pickup_longitude":"DOUBLE","pickup_latitude":"DOUBLE","dropoff_longitude":"DOUBLE","dropoff_latitude":"DOUBLE","rate_code_id":"VARCHAR","store_and_fwd_flag":"VARCHAR(1)","pu_location_id":"INTEGER","do_location_id":"INTEGER","payment_type_id":"VARCHAR","fare_amount":"DECIMAL(12,2)","extra":"DECIMAL(12,2)","mta_tax":"DECIMAL(12,2)","tip_amount":"DECIMAL(12,2)","tolls_amount":"DECIMAL(12,2)","improvement_surcharge":"DECIMAL(12,2)","total_amount":"DECIMAL(12,2)","congestion_surcharge":"DECIMAL(12,2)","airport_fee":"DECIMAL(12,2)","cbd_congestion_fee":"DECIMAL(12,2)"}


def schema_evolution(schema):
    lookup={(int(r.year),r.column_name):r for r in schema.itertuples()}; rows=[]
    for cf in CANONICAL:
        vals={}; types=set(); present=[]
        for y in YEARS:
            inv={KEYMAP[k]:v for k,v in CANON[y].items() if k in KEYMAP}
            src=inv.get(cf,""); rec=lookup.get((y,src)) if src else None
            vals[str(y)]=(f"{src} [{rec.logical_type}]" if rec else "—")
            if rec: types.add(rec.logical_type); present.append(y)
        change=[]
        names=[v.split(" [")[0] for v in vals.values() if v!="—"]
        if len(set(names))>1: change.append("renamed/case changed")
        if len(types)>1: change.append("type changed")
        if len(present)<len(YEARS): change.append("not present in all versions")
        rule="Keep source_field/source_file lineage; TRY_CAST to canonical type; null means structurally unavailable, not observed-null"
        if cf.endswith("location_id"): rule="Use native LocationID from 2015+; derive a separate spatial-match field from coordinates for 2009/2010; never overwrite raw"
        rows.append({"Canonical field":cf,**vals,"Thay đổi":"; ".join(change) or "stable","Quy tắc chuẩn hóa":rule})
    pd.DataFrame(rows).to_csv(PROFILES/"04_schema_evolution.csv",index=False,encoding="utf-8-sig")


def mapping(schema):
    rows=[]
    for r in schema.itertuples():
        y=int(r.year); rev={v:KEYMAP[k] for k,v in CANON[y].items() if k in KEYMAP}; cf=rev.get(r.column_name,"source_extra__"+r.column_name.lower())
        transform=f"TRY_CAST({r.column_name} AS {TYPES.get(cf,'VARCHAR')}); retain raw value and source lineage"
        target="fact_taxi_trip" if cf in CANONICAL else "stg_yellow_taxi_trip"
        tf=cf if target=="fact_taxi_trip" else cf
        dq="DQ-SCHEMA-001"
        if "datetime" in cf: dq="DQ-TIME-001..005"
        elif "location" in cf or "longitude" in cf or "latitude" in cf: dq="DQ-LOCATION-001..004 / DQ-SPATIAL-001..002"
        elif cf.endswith("amount") or cf in ("extra","mta_tax","improvement_surcharge","congestion_surcharge","airport_fee","cbd_congestion_fee"): dq="DQ-AMOUNT-001..007"
        rows.append({"Source file/version":r.file,"Source field":r.column_name,"Source type":r.logical_type,"Canonical field":cf,"Staging type":TYPES.get(cf,"VARCHAR"),"Target table":target,"Target field":tf,"Transformation":transform,"DQ rule":dq})
    pd.DataFrame(rows).to_csv(MAPPINGS/"12_source_to_target_mapping.csv",index=False,encoding="utf-8-sig")


def dq_rules(anom):
    defs=anom.sort_values(["rule_id","year"]).groupby("rule_id",as_index=False).first()
    agg=anom.groupby("rule_id",as_index=False).agg(checked_rows=("checked_rows","sum"),failed_rows=("failed_rows","sum"))
    out=defs[["rule_id","description","condition","severity","recommendation"]].merge(agg,on="rule_id")
    out["failed_rate"]=out.failed_rows/out.checked_rows
    out.insert(1,"group",out.rule_id.str.extract(r"DQ-([A-Z]+)-")[0]); out.insert(2,"field","see condition")
    out=out.rename(columns={"rule_id":"Mã rule","group":"Nhóm","field":"Trường","condition":"Điều kiện kiểm tra","severity":"Mức độ","failed_rows":"Số dòng lỗi","failed_rate":"Tỷ lệ","recommendation":"Cách xử lý","description":"Mô tả","checked_rows":"Số dòng kiểm tra"})
    extras=pd.DataFrame([
      {"Mã rule":"DQ-FILE-001","Nhóm":"FILE","Trường":"file","Điều kiện kiểm tra":"HTTP status not 200/206 or unreadable Parquet footer","Mức độ":"Critical","Số dòng lỗi":0,"Tỷ lệ":0,"Cách xử lý":"Reject file; retain log; retry official URL","Mô tả":"File inaccessible/corrupt","Số dòng kiểm tra":0},
      {"Mã rule":"DQ-SCHEMA-001","Nhóm":"SCHEMA","Trường":"schema","Điều kiện kiểm tra":"Actual field/type differs from version mapping","Mức độ":"Info","Số dòng lỗi":0,"Tỷ lệ":0,"Cách xử lý":"Apply versioned mapping; do not treat valid evolution as row error","Mô tả":"Schema evolution","Số dòng kiểm tra":0}])
    pd.concat([out,extras],ignore_index=True).to_csv(ANOMALIES/"09_data_quality_rules.csv",index=False,encoding="utf-8-sig")


def md_table(df,cols=None,n=20):
    x=df[cols] if cols else df; return x.head(n).to_markdown(index=False)


def report():
    mf=pd.read_csv(PROFILES/"01_yellow_taxi_file_manifest.csv"); fp=pd.read_csv(PROFILES/"02_parquet_file_profile.csv"); sc=pd.read_csv(PROFILES/"03_schema_by_file.csv"); zp=pd.read_csv(PROFILES/"05_taxi_zone_lookup_profile.csv"); zi=pd.read_csv(PROFILES/"06_taxi_zone_lookup_issues.csv"); sp=pd.read_csv(PROFILES/"07_shapefile_profile.csv"); cp=pd.read_csv(PROFILES/"08_csv_shapefile_comparison.csv"); an=pd.read_csv(ANOMALIES/"10_anomaly_summary.csv"); ds=pd.read_csv(ANOMALIES/"distribution_percentiles.csv")
    total=int(fp.total_rows.sum()); linked=int(mf.file_name.fillna("").ne("").sum()); missing=int(mf.file_name.fillna("").eq("").sum())
    top=an.sort_values("failed_rows",ascending=False)[["file","rule_id","checked_rows","failed_rows","failed_rate","severity"]].head(20).copy(); top.failed_rate=top.failed_rate.map(lambda x:f"{x:.4%}")
    shpmetrics=dict(zip(sp.metric,sp.value)); cmpissues=int(cp.issues.fillna("").ne("").sum())
    fee8=an[an.rule_id=="DQ-AMOUNT-008"][["year","failed_rows"]].astype(int).to_dict("records")
    fee9=an[an.rule_id=="DQ-AMOUNT-009"][["year","failed_rows"]].astype(int).to_dict("records")
    fee10=an[an.rule_id=="DQ-AMOUNT-010"][["year","failed_rows"]].astype(int).to_dict("records")
    text=f"""# NYC Yellow Taxi Source Survey and Data Quality Report

**Ngày truy cập/kiểm tra:** {CHECKED_AT} (Asia/Bangkok)  
**Nguồn chính:** {TLC_PAGE}  
**Phạm vi:** Yellow Taxi 2009-2026; 7 file đại diện; Taxi Zone CSV và Shapefile đính kèm.

## 1. Executive Summary

Khảo sát đọc metadata của 7 file Parquet đại diện và chạy truy vấn tổng hợp toàn bộ **{total:,} dòng**. Trang TLC cung cấp **{linked}** link tháng Yellow Taxi trong khung 2009-2026 tại ngày kiểm tra; **{missing}** vị trí năm-tháng chưa có link trên trang, không được tự diễn giải là mất dữ liệu nếu đó là tháng tương lai/chưa công bố. TLC lưu ý dữ liệu do nhà cung cấp công nghệ gửi và không bảo đảm độ chính xác.

Các vị trí không có anchor trong HTML crawl: 2023-05, 2023-07 đến 2023-11; 2024-01 đến 2024-03; và 2026-10 đến 2026-12. Đây là kết quả crawl trang tại ngày kiểm tra, không phải khẳng định file vật lý không tồn tại; cần điều tra riêng nếu backfill production.

Schema không ổn định xuyên suốt giai đoạn. Hai file 2009/2010 dùng tọa độ; từ file 2015 đại diện dùng `PULocationID`/`DOLocationID`. `Airport_fee` khác chữ hoa so với dictionary; `cbd_congestion_fee` chỉ có từ 2025 trong mẫu. Vì vậy Staging phải version-aware và giữ lineage, không union theo vị trí cột.

## 2. Phạm vi và phương pháp

- Crawl duy nhất trang TLC và URL liên kết trực tiếp; manifest chứa cả ô tháng không có link.
- Chọn tháng 1 nếu manifest có; 2024 dùng tháng 4 vì tháng 1-3 không được parser manifest nhận diện tại lần crawl, đã ghi selection reason.
- PyArrow đọc footer, schema, row-group statistics. DuckDB quét toàn file để tính DQ và percentile. Không sửa raw.
- Ngưỡng duration/distance/speed dùng P99 quan sát của từng file như cờ thống kê; không khẳng định mọi dòng trên P99 là sai.
- Domain mã theo phiên bản; domain dictionary 2025 không áp ngược cho 2009/2010.

Tài liệu đồ án yêu cầu phân tích chuyến/hành khách theo thời gian, doanh thu theo borough/vendor, payment, pickup/dropoff, nhóm hành khách/rate/distance, tuyến phổ biến, tip và tốc độ. Các yêu cầu này là căn cứ cho grain, fact và dimension dưới đây.

## 3. Danh mục nguồn

| Nguồn | URL tải/chứng cứ | Vai trò |
|---|---|---|
| TLC Trip Record Data | {TLC_PAGE} | Trang gốc/crawl manifest |
| Yellow dictionary (18-03-2025) | https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf | Ý nghĩa trường và domain hiện hành |
| Trip Record User Guide | https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf | Grain và lịch sử nguồn |
| Working With Parquet | https://www.nyc.gov/assets/tlc/downloads/pdf/working_parquet_format.pdf | Hướng dẫn định dạng |
| Taxi Zone lookup | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv | Dimension mô tả |
| Taxi Zone shapefile | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip | Geometry không gian |

## 4. Khảo sát Yellow Taxi Parquet

{md_table(fp,["file","size_bytes","row_groups","total_rows","num_columns","read_metadata_seconds","status"],10)}

Chi tiết từng cột, physical/logical type, nullable, min/max và null count từ metadata nằm ở `03_schema_by_file.csv`. Nullability của Arrow là khả năng schema, khác với null count quan sát.

## 5. Schema evolution 2009-2026

- 2009: 18 cột, tên như `vendor_name`, `Trip_Pickup_DateTime`, tọa độ `Start_Lon/Lat`; datetime vật lý/logical là string.
- 2010: 18 cột nhưng đổi tên/case (`vendor_id`, `pickup_datetime`, `pickup_longitude`); rate từ double sang string trong mẫu.
- 2015: 19 cột; datetime timestamp, LocationID thay tọa độ, có improvement surcharge; congestion/airport là logical null.
- 2019: passenger/rate đổi sang double; congestion có double, airport vẫn null.
- 2024: integer width thay đổi; `store_and_fwd_flag` large_string; `Airport_fee` viết hoa A.
- 2025-2026: 20 cột và thêm `cbd_congestion_fee`, phù hợp thông báo TLC về dữ liệu từ 2025.

Bảng canonical đầy đủ: `04_schema_evolution.csv`. Quy tắc cốt lõi: cast vào cột canonical nhưng đồng thời giữ `source_file`, `source_year_month`, `source_field`, raw value khi cast lỗi và `schema_version`.

## 6. Taxi Zone CSV

CSV có 265 dòng, 4 cột. `LocationID` phải được kiểm chứng theo membership; không suy ra hợp lệ từ min/max. Có {len(zi)} dòng issue/special-case được ghi trong `06_taxi_zone_lookup_issues.csv`; một phần là airport/Unknown/N/A/Outside NYC được chủ động liệt kê, không đồng nghĩa lỗi.

## 7. Taxi Zone Shapefile

- Feature: **{shpmetrics.get('feature_count')}**; geometry: **{shpmetrics.get('geometry_types')}**; CRS: **{shpmetrics.get('crs')}**.
- Geometry null/empty/invalid: **{shpmetrics.get('geometry_null')}/{shpmetrics.get('geometry_empty')}/{shpmetrics.get('geometry_invalid')}**.
- CSV có 265 dòng, shapefile 263 feature; bảng outer comparison có **{cmpissues}** dòng mang cờ khác biệt hoặc chỉ ở một nguồn.
- CRS nguồn phải được bảo toàn; tạo derivative EPSG:4326 cho web map. Spatial join nên thực hiện trong CRS phù hợp và dùng `covers` nếu muốn tính điểm trên biên.

## 8. Liên kết ba nguồn

```mermaid
flowchart LR
  T[Yellow Taxi Trip] -->|PULocationID| P[dim_zone: Pickup role]
  T -->|DOLocationID| D[dim_zone: Dropoff role]
  C[Zone CSV: attributes] -->|LocationID| Z[Conformed dim_zone]
  S[Shapefile: polygon] -->|LocationID| Z
  O[2009/2010 lon-lat] -->|WGS84 point; CRS transform; point-in-polygon| Z
  Z --> M[Map dashboard]
```

Với dữ liệu cũ: kiểm tra/null-zero/range tọa độ, tạo Point EPSG:4326, chuyển polygon về cùng CRS, point-in-polygon, ghi `spatial_match_count`, `derived_location_id`, cờ unmatched/multiple/boundary. Không tự gán điểm ngoài polygon.

## 9. Dị thường dữ liệu

Top kết quả theo số dòng vi phạm (các rule P99 là cờ thống kê, luôn xấp xỉ 1%):

{md_table(top,n=20)}

Percentile theo file:

{md_table(ds,["file","dur_p50","dur_p90","dur_p95","dur_p99","dur_max","dist_p99","speed_p99","speed_max"],10)}

`10_anomaly_summary.csv` chứa điều kiện SQL, checked/failed/rate/severity/recommendation cho từng file. `11_anomaly_samples.parquet` có tối đa 3 mẫu cho mỗi core rule/file và tối đa 10 mẫu không gian cho mỗi vai trò pickup/dropoff. Giá trị âm được gắn cờ để điều tra đảo/điều chỉnh, không tự xóa. Tổng tiền được so ở tolerance 0.01 và 0.05 USD, chỉ cộng cột tồn tại (cột cấu trúc không tồn tại thành NULL rồi COALESCE 0). Tỷ lệ mismatch tiền cao ở 2024-2026 tập trung cả ở bản ghi có các thành phần phí null (ví dụ payment/rate null); do đó đây là cờ reconciliation cần điều tra chứ chưa đủ căn cứ kết luận `total_amount` sai.

Kiểm tra phí theo thời điểm/vị trí: airport fee dương ngoài pickup zone JFK/LGA theo năm là `{fee8}`; CBD fee dương trước 2025-01-05 là `{fee9}`; improvement/congestion surcharge trước ngày hiệu lực là `{fee10}`. Các dòng này cần xem timestamp ngoại kỳ và semantics do vendor trước khi kết luận sai phí.

## 10. Data Quality Rules

Rule registry ở `09_data_quality_rules.csv`; nhóm FILE, SCHEMA, TIME, PASSENGER, DISTANCE, LOCATION, CODE, AMOUNT, DUP và SPATIAL. `Critical/High` không đồng nghĩa xóa; pipeline mặc định quarantine hoặc giữ-và-gắn-cờ theo ảnh hưởng.

## 11. Canonical schema và Staging

Staging tối thiểu gồm canonical fields trong `04_schema_evolution.csv`, cộng `source_file`, `source_row_group`, `ingested_at`, `schema_version`, `record_hash`, `cast_error_flags`, `dq_flags`, tọa độ gốc và LocationID native/derived tách biệt. `record_hash` chỉ phục vụ phát hiện ứng viên trùng, không phải trip ID chính thức.

## 12. Grain, fact và dimension sơ bộ

**Grain:** một dòng fact đại diện một bản ghi chuyến Yellow Taxi hoàn tất từ meter-on đến meter-off; những dòng không chứng minh được hoàn tất được giữ ở quarantine/staging, không ép vào trusted fact.

`fact_taxi_trip`: surrogate key; pickup/dropoff date/time/zone keys; vendor/payment/rate keys; trip_count=1; passenger_count; distance; duration; speed; fare/extra/tax/tip/tolls/surcharges/total; source lineage; DQ flags. Role-playing: Pickup/Dropoff Date, Time và Zone.

Dimensions có bằng chứng từ yêu cầu phân tích: `dim_date` (day-month-year, weekday), `dim_time` (hour bands), `dim_zone` (borough/zone/service zone/geometry reference), `dim_vendor`, `dim_payment_type`, `dim_rate_code`. Domain version/SCD cần thiết nếu nghĩa mã thay đổi theo thời gian.

## 13. Kiến trúc đề xuất

```mermaid
flowchart LR
  A[Official raw + checksums] --> B[Versioned Staging + lineage]
  B --> Q[DQ quarantine/flags]
  B --> C[Conformed DDS dimensions]
  C --> F[fact_taxi_trip]
  F --> R[OLAP / Power BI / map]
  Z[Zone CSV + shape] --> C
```

## 14. Hạn chế

- Bảy tháng đại diện không chứng minh chất lượng của toàn bộ 2009-2026; cần chạy cùng rule trên mọi tháng trước production.
- Data dictionary hiện hành không mô tả đầy đủ domain lịch sử 2009/2010; cần tài liệu versioned hoặc xác nhận nghiệp vụ.
- Phần spatial row-level trên 2009/2010 nên chạy batch riêng và kiểm định predicate (`within` so với `covers`) trước khi nạp chính thức.
- User Guide 2019 còn mô tả CSV/cadence cũ, trong khi trang hiện tại nói Parquet và công bố hàng tháng; ưu tiên trạng thái trang tại ngày truy cập.

## 15. Kết luận và quyết định cần thống nhất

Nhóm cần chốt: (1) ngưỡng thời lượng/vận tốc nghiệp vụ sau khi xem percentile; (2) policy passenger >6 và zero; (3) xử lý giao dịch âm/đảo; (4) tolerance cân bằng tiền; (5) predicate spatial và Unknown key; (6) duplicate survivorship; (7) domain mã theo thời gian; (8) whether revenue uses pickup or dropoff zone; (9) SCD strategy cho dimensions.

## Tài liệu tham khảo

Các URL chính thức nêu ở mục 3; truy cập {CHECKED_AT}. Số liệu trong báo cáo được sinh từ các file output và raw có SHA-256 trong `representative_files.csv`.
"""
    (REPORTS/"NYC_Yellow_Taxi_Source_Survey_and_Data_Quality_Report.md").write_text(text,encoding="utf-8")


def main():
    init_dirs(); log=logger("generate_deliverables")
    schema=pd.read_csv(PROFILES/"03_schema_by_file.csv"); anom=pd.read_csv(ANOMALIES/"10_anomaly_summary.csv")
    schema_evolution(schema); mapping(schema); dq_rules(anom); report(); log.info("Generated schema evolution, DQ registry, mapping and report")


if __name__=="__main__": main()
