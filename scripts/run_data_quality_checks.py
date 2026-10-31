from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common import ANOMALIES, PROFILES, RAW, REF, init_dirs, logger


CANON = {
2009:{"vendor":"vendor_name","pickup":"Trip_Pickup_DateTime","dropoff":"Trip_Dropoff_DateTime","passenger":"Passenger_Count","distance":"Trip_Distance","pu_lon":"Start_Lon","pu_lat":"Start_Lat","do_lon":"End_Lon","do_lat":"End_Lat","rate":"Rate_Code","store":"store_and_forward","payment":"Payment_Type","fare":"Fare_Amt","extra":"surcharge","mta":"mta_tax","tip":"Tip_Amt","tolls":"Tolls_Amt","total":"Total_Amt"},
2010:{"vendor":"vendor_id","pickup":"pickup_datetime","dropoff":"dropoff_datetime","passenger":"passenger_count","distance":"trip_distance","pu_lon":"pickup_longitude","pu_lat":"pickup_latitude","do_lon":"dropoff_longitude","do_lat":"dropoff_latitude","rate":"rate_code","store":"store_and_fwd_flag","payment":"payment_type","fare":"fare_amount","extra":"surcharge","mta":"mta_tax","tip":"tip_amount","tolls":"tolls_amount","total":"total_amount"},
2015:{"vendor":"VendorID","pickup":"tpep_pickup_datetime","dropoff":"tpep_dropoff_datetime","passenger":"passenger_count","distance":"trip_distance","rate":"RatecodeID","store":"store_and_fwd_flag","pu":"PULocationID","do":"DOLocationID","payment":"payment_type","fare":"fare_amount","extra":"extra","mta":"mta_tax","tip":"tip_amount","tolls":"tolls_amount","improvement":"improvement_surcharge","total":"total_amount","congestion":"congestion_surcharge","airport":"airport_fee"},
2019:{"vendor":"VendorID","pickup":"tpep_pickup_datetime","dropoff":"tpep_dropoff_datetime","passenger":"passenger_count","distance":"trip_distance","rate":"RatecodeID","store":"store_and_fwd_flag","pu":"PULocationID","do":"DOLocationID","payment":"payment_type","fare":"fare_amount","extra":"extra","mta":"mta_tax","tip":"tip_amount","tolls":"tolls_amount","improvement":"improvement_surcharge","total":"total_amount","congestion":"congestion_surcharge","airport":"airport_fee"},
2024:{"vendor":"VendorID","pickup":"tpep_pickup_datetime","dropoff":"tpep_dropoff_datetime","passenger":"passenger_count","distance":"trip_distance","rate":"RatecodeID","store":"store_and_fwd_flag","pu":"PULocationID","do":"DOLocationID","payment":"payment_type","fare":"fare_amount","extra":"extra","mta":"mta_tax","tip":"tip_amount","tolls":"tolls_amount","improvement":"improvement_surcharge","total":"total_amount","congestion":"congestion_surcharge","airport":"Airport_fee"},
2025:{"vendor":"VendorID","pickup":"tpep_pickup_datetime","dropoff":"tpep_dropoff_datetime","passenger":"passenger_count","distance":"trip_distance","rate":"RatecodeID","store":"store_and_fwd_flag","pu":"PULocationID","do":"DOLocationID","payment":"payment_type","fare":"fare_amount","extra":"extra","mta":"mta_tax","tip":"tip_amount","tolls":"tolls_amount","improvement":"improvement_surcharge","total":"total_amount","congestion":"congestion_surcharge","airport":"Airport_fee","cbd":"cbd_congestion_fee"},
2026:{"vendor":"VendorID","pickup":"tpep_pickup_datetime","dropoff":"tpep_dropoff_datetime","passenger":"passenger_count","distance":"trip_distance","rate":"RatecodeID","store":"store_and_fwd_flag","pu":"PULocationID","do":"DOLocationID","payment":"payment_type","fare":"fare_amount","extra":"extra","mta":"mta_tax","tip":"tip_amount","tolls":"tolls_amount","improvement":"improvement_surcharge","total":"total_amount","congestion":"congestion_surcharge","airport":"Airport_fee","cbd":"cbd_congestion_fee"},
}

SEV = {"DQ-TIME-001":"High","DQ-TIME-002":"High","DQ-TIME-003":"Medium","DQ-TIME-004":"Medium","DQ-TIME-005":"High","DQ-PASSENGER-001":"Medium","DQ-PASSENGER-002":"High","DQ-PASSENGER-003":"Medium","DQ-PASSENGER-004":"Medium","DQ-DISTANCE-001":"Medium","DQ-DISTANCE-002":"High","DQ-DISTANCE-003":"Medium","DQ-DISTANCE-004":"High","DQ-DISTANCE-005":"High","DQ-LOCATION-001":"High","DQ-LOCATION-002":"High","DQ-LOCATION-003":"High","DQ-LOCATION-004":"Medium","DQ-CODE-001":"Medium","DQ-CODE-002":"Medium","DQ-CODE-003":"Medium","DQ-CODE-004":"Low","DQ-AMOUNT-001":"High","DQ-AMOUNT-002":"Medium","DQ-AMOUNT-003":"High","DQ-AMOUNT-004":"High","DQ-AMOUNT-005":"Medium","DQ-AMOUNT-006":"Medium","DQ-AMOUNT-007":"Medium","DQ-AMOUNT-008":"High","DQ-AMOUNT-009":"High","DQ-AMOUNT-010":"High","DQ-DUP-001":"High","DQ-DUP-002":"Medium","DQ-SPATIAL-001":"Medium","DQ-SPATIAL-002":"High"}
REC = {"High":"Quarantine; investigate before loading trusted fact","Medium":"Keep and flag; exclude from affected KPI by governed rule","Low":"Standardize representation; retain raw value"}


def qi(s): return '"'+s.replace('"','""')+'"'
def val(m,k,typ="DOUBLE"):
    return f"TRY_CAST({qi(m[k])} AS {typ})" if k in m else f"CAST(NULL AS {typ})"


def base_sql(path: Path, y:int):
    m=CANON[y]
    fields={
      "vendor":val(m,"vendor","VARCHAR"),"pickup":val(m,"pickup","TIMESTAMP"),"dropoff":val(m,"dropoff","TIMESTAMP"),
      "passenger":val(m,"passenger"),"distance":val(m,"distance"),"rate":val(m,"rate","VARCHAR"),"store":val(m,"store","VARCHAR"),"payment":val(m,"payment","VARCHAR"),
      "pu":val(m,"pu"),"do_loc":val(m,"do"),"pu_lon":val(m,"pu_lon"),"pu_lat":val(m,"pu_lat"),"do_lon":val(m,"do_lon"),"do_lat":val(m,"do_lat"),
      "fare":val(m,"fare"),"extra":val(m,"extra"),"mta":val(m,"mta"),"tip":val(m,"tip"),"tolls":val(m,"tolls"),"improvement":val(m,"improvement"),"congestion":val(m,"congestion"),"airport":val(m,"airport"),"cbd":val(m,"cbd"),"total":val(m,"total")}
    expected=" + ".join(f"coalesce({c},0)" for c in ["fare","extra","mta","tip","tolls","improvement","congestion","airport","cbd"])
    select=",\n".join(f"{v} AS {k}" for k,v in fields.items())
    p=str(path).replace("'","''")
    return f"WITH b AS (SELECT {select} FROM read_parquet('{p}')), d AS (SELECT *, date_diff('second',pickup,dropoff)/60.0 duration_minutes, CASE WHEN dropoff>pickup AND distance>=0 THEN distance/(date_diff('second',pickup,dropoff)/3600.0) END average_speed_mph, {expected} expected_total FROM b)"


def main():
    init_dirs(); log=logger("run_data_quality_checks")
    reps=pd.read_csv(PROFILES/"representative_files.csv"); zones=pd.read_csv(REF/"taxi+_zone_lookup.csv"); ids=",".join(map(str,zones.LocationID.astype(int)))
    con=duckdb.connect(); con.execute("SET threads=4; SET preserve_insertion_order=false")
    summaries=[]; samples=[]; diststats=[]
    for _,r in reps.iterrows():
        y=int(r.year); mo=int(r.month); path=RAW/r.file_name; cte=base_sql(path,y)
        # Data-driven thresholds are the observed P99 in each file, reported separately.
        stats=con.execute(cte+" SELECT count(*) n, quantile_cont(duration_minutes,0.5) dur_p50, quantile_cont(duration_minutes,0.9) dur_p90, quantile_cont(duration_minutes,0.95) dur_p95, quantile_cont(duration_minutes,0.99) dur_p99, max(duration_minutes) dur_max, quantile_cont(distance,0.5) dist_p50, quantile_cont(distance,0.9) dist_p90, quantile_cont(distance,0.95) dist_p95, quantile_cont(distance,0.99) dist_p99, max(distance) dist_max, quantile_cont(average_speed_mph,0.5) speed_p50, quantile_cont(average_speed_mph,0.9) speed_p90, quantile_cont(average_speed_mph,0.95) speed_p95, quantile_cont(average_speed_mph,0.99) speed_p99, max(average_speed_mph) speed_max FROM d").fetchdf().iloc[0].to_dict()
        n=int(stats["n"]); diststats.append({"file":r.file_name,"year":y,"month":mo,**stats})
        rules=[
          ("DQ-TIME-001","pickup IS NULL OR dropoff IS NULL","Pickup/dropoff datetime null or unparseable"),("DQ-TIME-002","dropoff < pickup","Dropoff precedes pickup / negative duration"),("DQ-TIME-003","dropoff = pickup","Zero duration"),("DQ-TIME-004",f"pickup IS NOT NULL AND (year(pickup)<>{y} OR month(pickup)<>{mo})","Pickup outside file year-month"),("DQ-TIME-005",f"duration_minutes > {float(stats['dur_p99'] or 0)}","Duration above empirical file P99"),
          ("DQ-PASSENGER-001","passenger IS NULL","Passenger count null"),("DQ-PASSENGER-002","passenger < 0 OR passenger <> trunc(passenger)","Passenger count negative or non-integer"),("DQ-PASSENGER-003","passenger = 0","Passenger count zero"),("DQ-PASSENGER-004","passenger > 6","Passenger count >6; business threshold requiring confirmation"),
          ("DQ-DISTANCE-001","distance IS NULL","Trip distance null"),("DQ-DISTANCE-002","distance < 0","Negative trip distance"),("DQ-DISTANCE-003","distance = 0","Zero trip distance"),("DQ-DISTANCE-004",f"distance > {float(stats['dist_p99'] or 0)}","Distance above empirical file P99"),("DQ-DISTANCE-005",f"average_speed_mph > {float(stats['speed_p99'] or 0)}","Average speed above empirical file P99"),
          ("DQ-CODE-001","vendor IS NULL","Vendor null"),("DQ-CODE-002","rate IS NULL","Rate code null"),("DQ-CODE-003","payment IS NULL","Payment type null"),
          ("DQ-AMOUNT-001","fare IS NULL OR fare < 0","Fare null or negative"),("DQ-AMOUNT-002","extra < 0 OR mta < 0 OR tip < 0 OR tolls < 0 OR improvement < 0 OR congestion < 0 OR airport < 0 OR cbd < 0","One or more itemized amounts negative"),("DQ-AMOUNT-003","total IS NULL OR total <= 0","Total null, zero or negative"),("DQ-AMOUNT-004","tip > total","Tip exceeds total"),("DQ-AMOUNT-006","abs(expected_total-total) > 0.01","Item sum differs from total by > $0.01"),("DQ-AMOUNT-007","abs(expected_total-total) > 0.05","Item sum differs from total by > $0.05")]
        if "pu" in CANON[y]:
            rules += [("DQ-LOCATION-001","pu IS NULL OR do_loc IS NULL","Pickup/dropoff LocationID null"),("DQ-LOCATION-002",f"(pu IS NOT NULL AND pu NOT IN ({ids})) OR (do_loc IS NOT NULL AND do_loc NOT IN ({ids}))","LocationID not found in lookup (direct membership, not min/max)"),("DQ-LOCATION-003",f"pu <> trunc(pu) OR do_loc <> trunc(do_loc)","LocationID non-integer"),("DQ-LOCATION-004","pu IN (264,265) OR do_loc IN (264,265)","Unknown/N/A or Outside NYC lookup member")]
        # Versioned code domains; no current-domain backcasting to 2009/2010 string codes.
        if y in (2015,2019): rules += [("DQ-CODE-005","vendor IS NOT NULL AND vendor NOT IN ('1','2')","Vendor outside period-scoped numeric domain 1,2"),("DQ-CODE-006","payment IS NOT NULL AND payment NOT IN ('1','2','3','4','5','6')","Payment outside period-scoped domain 1-6")]
        if y>=2024: rules += [("DQ-CODE-005","vendor IS NOT NULL AND vendor NOT IN ('1','2','6','7')","Vendor outside current dictionary domain 1,2,6,7"),("DQ-CODE-006",("payment IS NOT NULL AND payment NOT IN ('0','1','2','3','4','5','6')" if y>=2025 else "payment IS NOT NULL AND payment NOT IN ('1','2','3','4','5','6')"),"Payment outside period-scoped domain")]
        if y>=2015: rules += [("DQ-CODE-004","store IS NOT NULL AND upper(trim(store)) NOT IN ('Y','N')","Store-and-forward outside period documented Y/N"),("DQ-AMOUNT-005","tip > 0 AND payment <> '1'","Positive recorded electronic tip with non-credit numeric payment code")]
        if "airport" in CANON[y] and "pu" in CANON[y]: rules += [("DQ-AMOUNT-008","airport > 0 AND pu NOT IN (132,138)","Airport fee positive but pickup zone is neither JFK (132) nor LaGuardia (138)")]
        if "cbd" in CANON[y]: rules += [("DQ-AMOUNT-009","cbd > 0 AND pickup < TIMESTAMP '2025-01-05 00:00:00'","CBD congestion fee before official 2025-01-05 effective date")]
        if "improvement" in CANON[y] or "congestion" in CANON[y]: rules += [("DQ-AMOUNT-010","(improvement > 0 AND pickup < TIMESTAMP '2015-01-01 00:00:00') OR (congestion > 0 AND pickup < TIMESTAMP '2019-01-01 00:00:00')","Improvement/congestion surcharge before official effective date")]
        counts=con.execute(cte+" SELECT "+", ".join(f"count(*) FILTER (WHERE {expr}) AS r{i}" for i,(_,expr,_) in enumerate(rules))+" FROM d").fetchone()
        for i,(rid,expr,desc) in enumerate(rules):
            failed=int(counts[i]); sev=SEV.get(rid,"Medium")
            summaries.append(dict(file=r.file_name,year=y,month=mo,rule_id=rid,checked_rows=n,failed_rows=failed,failed_rate=failed/n if n else 0,severity=sev,recommendation=REC.get(sev,"Keep and flag"),condition=expr,description=desc))
            if failed:
                try:
                    sdf=con.execute(cte+f" SELECT * EXCLUDE(expected_total) FROM d WHERE {expr} LIMIT 3").fetchdf()
                    for row in sdf.to_dict("records"): samples.append(dict(file=r.file_name,year=y,month=mo,rule_id=rid,sample_json=json.dumps(row,default=str,ensure_ascii=False)))
                except Exception as e: log.warning("sample %s %s",rid,e)
        # Exact duplicate counts by complete source row and by canonical business composite key.
        p=str(path).replace("'","''")
        full=con.execute(f"SELECT coalesce(sum(c-1),0) FROM (SELECT *, count(*) c FROM read_parquet('{p}') GROUP BY ALL HAVING count(*)>1)").fetchone()[0]
        comp=con.execute(cte+' SELECT coalesce(sum(c-1),0) FROM (SELECT count(*) c FROM d GROUP BY vendor,pickup,dropoff,pu,do_loc,pu_lon,pu_lat,do_lon,do_lat,passenger,distance,total HAVING count(*)>1)').fetchone()[0]
        for rid,failed,desc,sev in [("DQ-DUP-001",full,"Exact full-row duplicates","High"),("DQ-DUP-002",comp,"Duplicate canonical composite-key candidates; record hash is not an official trip ID","Medium")]: summaries.append(dict(file=r.file_name,year=y,month=mo,rule_id=rid,checked_rows=n,failed_rows=int(failed),failed_rate=float(failed)/n if n else 0,severity=sev,recommendation="Quarantine duplicate candidates; do not auto-delete",condition="exact GROUP BY",description=desc))
        log.info("DQ complete %s rows=%s",r.file_name,n)
    pd.DataFrame(summaries).to_csv(ANOMALIES/"10_anomaly_summary.csv",index=False,encoding="utf-8-sig")
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(samples),preserve_index=False),ANOMALIES/"11_anomaly_samples.parquet",compression="zstd")
    pd.DataFrame(diststats).to_csv(ANOMALIES/"distribution_percentiles.csv",index=False,encoding="utf-8-sig")


if __name__ == "__main__": main()
