from __future__ import annotations

import pandas as pd

from common import PROFILES, REF, init_dirs, logger


def main():
    init_dirs(); log=logger("profile_taxi_zone_csv")
    p=REF/"taxi+_zone_lookup.csv"; df=pd.read_csv(p)
    prof=[]
    for c in df.columns:
        s=df[c]; prof.append(dict(source=p.name,metric_type="column",column=c,row_count=len(df),column_count=len(df.columns),dtype=str(s.dtype),null_count=int(s.isna().sum()),null_rate=float(s.isna().mean()),distinct_count=int(s.nunique(dropna=True)),min=(s.min() if pd.api.types.is_numeric_dtype(s) else ""),max=(s.max() if pd.api.types.is_numeric_dtype(s) else ""),is_unique=bool(s.is_unique)))
    for group,col in [("borough_distribution","Borough"),("service_zone_distribution","service_zone")]:
        for val,n in df[col].fillna("<NULL>").value_counts(dropna=False).items():
            prof.append(dict(source=p.name,metric_type=group,column=col,row_count=len(df),column_count=len(df.columns),dtype="",null_count="",null_rate="",distinct_count="",min="",max="",is_unique="",value=val,count=int(n)))
    issues=[]
    def add(rule,detail,rows):
        for _,x in rows.iterrows(): issues.append(dict(rule_id=rule,issue=detail,LocationID=x.get("LocationID",""),Borough=x.get("Borough",""),Zone=x.get("Zone",""),service_zone=x.get("service_zone","")))
    add("ZONE-DUP-ID","Duplicate LocationID",df[df.LocationID.duplicated(False)])
    add("ZONE-DUP-NAME","Same Zone name assigned to multiple LocationIDs",df[df.Zone.duplicated(False)].sort_values("Zone"))
    stripmask=pd.Series(False,index=df.index)
    for c in ["Borough","Zone","service_zone"]: stripmask |= df[c].astype(str).ne(df[c].astype(str).str.strip())
    add("ZONE-WHITESPACE","Leading/trailing whitespace",df[stripmask])
    allids=set(range(int(df.LocationID.min()),int(df.LocationID.max())+1)); missing=sorted(allids-set(df.LocationID.astype(int)))
    for x in missing: issues.append(dict(rule_id="ZONE-ID-GAP",issue="Missing integer inside observed min/max sequence",LocationID=x,Borough="",Zone="",service_zone=""))
    special=df[df.apply(lambda r:any(k in " ".join(map(str,r)).lower() for k in ["unknown","outside","n/a","airport"]),axis=1)]
    add("ZONE-SPECIAL","Unknown/Outside/Airport or equivalent",special)
    # Direct empirical consistency check: Manhattan must be Yellow Zone; EWR is Airports.
    mismatch=df[((df.Borough=="Manhattan") & (df.service_zone!="Yellow Zone")) | ((df.Borough=="EWR") & (df.service_zone!="Airports"))]
    add("ZONE-SERVICE-CHECK","Borough/service_zone inconsistency under explicit rules",mismatch)
    pd.DataFrame(prof).to_csv(PROFILES/"05_taxi_zone_lookup_profile.csv",index=False,encoding="utf-8-sig")
    pd.DataFrame(issues,columns=["rule_id","issue","LocationID","Borough","Zone","service_zone"]).to_csv(PROFILES/"06_taxi_zone_lookup_issues.csv",index=False,encoding="utf-8-sig")
    log.info("CSV rows=%s cols=%s issues=%s",len(df),len(df.columns),len(issues))


if __name__ == "__main__": main()
