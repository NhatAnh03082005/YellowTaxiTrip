import json
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from common import ANOMALIES, PROFILES, RAW, logger
from run_data_quality_checks import CANON, base_sql

log=logger("enrich_temporal_fee_checks"); reps=pd.read_csv(PROFILES/"representative_files.csv"); a=pd.read_csv(ANOMALIES/"10_anomaly_summary.csv"); s=pq.read_table(ANOMALIES/"11_anomaly_samples.parquet").to_pandas(); con=duckdb.connect(); added=[]
for _,r in reps.iterrows():
    y=int(r.year); cte=base_sql(RAW/r.file_name,y); n=int(a[a.file==r.file_name].checked_rows.iloc[0]); rules=[]
    if "airport" in CANON[y] and "pu" in CANON[y]: rules.append(("DQ-AMOUNT-008","airport > 0 AND pu NOT IN (132,138)","Airport fee positive outside JFK/LGA pickup zones"))
    if "cbd" in CANON[y]: rules.append(("DQ-AMOUNT-009","cbd > 0 AND pickup < TIMESTAMP '2025-01-05 00:00:00'","CBD fee before official 2025-01-05 effective date"))
    if "improvement" in CANON[y] or "congestion" in CANON[y]: rules.append(("DQ-AMOUNT-010","(improvement > 0 AND pickup < TIMESTAMP '2015-01-01 00:00:00') OR (congestion > 0 AND pickup < TIMESTAMP '2019-01-01 00:00:00')","Surcharge before official effective date"))
    for rid,expr,desc in rules:
        failed=int(con.execute(cte+f" SELECT count(*) FROM d WHERE {expr}").fetchone()[0]); a=a[~((a.file==r.file_name)&(a.rule_id==rid))]
        a.loc[len(a)]={"file":r.file_name,"year":y,"month":int(r.month),"rule_id":rid,"checked_rows":n,"failed_rows":failed,"failed_rate":failed/n,"severity":"High","recommendation":"Quarantine and verify fee/date/location semantics","condition":expr,"description":desc}
        if failed:
            for row in con.execute(cte+f" SELECT * EXCLUDE(expected_total) FROM d WHERE {expr} LIMIT 3").fetchdf().to_dict("records"): added.append({"file":r.file_name,"year":y,"month":int(r.month),"rule_id":rid,"sample_json":json.dumps(row,default=str)})
        log.info("%s %s=%s",r.file_name,rid,failed)
a.to_csv(ANOMALIES/"10_anomaly_summary.csv",index=False,encoding="utf-8-sig"); s=s[~s.rule_id.isin(["DQ-AMOUNT-008","DQ-AMOUNT-009","DQ-AMOUNT-010"])]; s=pd.concat([s,pd.DataFrame(added)],ignore_index=True); pq.write_table(pa.Table.from_pandas(s,preserve_index=False),ANOMALIES/"11_anomaly_samples.parquet",compression="zstd")
