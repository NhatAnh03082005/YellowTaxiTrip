import duckdb
import pandas as pd
from common import ANOMALIES, PROFILES, RAW, logger
from run_data_quality_checks import base_sql

log=logger("recompute_duplicate_counts")
reps=pd.read_csv(PROFILES/"representative_files.csv")
a=pd.read_csv(ANOMALIES/"10_anomaly_summary.csv")
con=duckdb.connect(); con.execute("SET threads=4; SET preserve_insertion_order=false")
for _,r in reps.iterrows():
    y=int(r.year); p=RAW/r.file_name; ps=str(p).replace("'","''"); cte=base_sql(p,y)
    full=int(con.execute(f"SELECT coalesce(sum(c-1),0) FROM (SELECT *, count(*) c FROM read_parquet('{ps}') GROUP BY ALL HAVING count(*)>1)").fetchone()[0])
    comp=int(con.execute(cte+' SELECT coalesce(sum(c-1),0) FROM (SELECT count(*) c FROM d GROUP BY vendor,pickup,dropoff,pu,do_loc,pu_lon,pu_lat,do_lon,do_lat,passenger,distance,total HAVING count(*)>1)').fetchone()[0])
    for rid,v in [("DQ-DUP-001",full),("DQ-DUP-002",comp)]:
        mask=(a.file==r.file_name)&(a.rule_id==rid); a.loc[mask,"failed_rows"]=v; a.loc[mask,"failed_rate"]=v/a.loc[mask,"checked_rows"]
    log.info("%s full=%s composite=%s",r.file_name,full,comp)
a=a[~((a.year.isin([2009,2010])) & (a.rule_id.isin(["DQ-CODE-004","DQ-AMOUNT-005"])))]
a.to_csv(ANOMALIES/"10_anomaly_summary.csv",index=False,encoding="utf-8-sig")
