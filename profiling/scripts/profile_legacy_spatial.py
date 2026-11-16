from __future__ import annotations

import json
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import geopandas as gpd
import shapely
from shapely.strtree import STRtree

from common import ANOMALIES, PROFILES, RAW, REF, init_dirs, logger

COORDS={2009:("Start_Lon","Start_Lat","End_Lon","End_Lat"),2010:("pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude")}


def scan_side(pf, loncol, latcol, tree, batch_size=250_000):
    totals=dict(checked=0,null=0,zero=0,out_of_range=0,swapped_like=0,valid=0,unmatched=0,multi=0,boundary=0,matched_once=0); samples=[]
    for batch in pf.iter_batches(columns=[loncol,latcol],batch_size=batch_size):
        lon=batch.column(0).to_numpy(zero_copy_only=False).astype(float); lat=batch.column(1).to_numpy(zero_copy_only=False).astype(float); n=len(lon); totals["checked"]+=n
        null=~np.isfinite(lon)|~np.isfinite(lat); zero=(lon==0)&(lat==0); oor=(lon < -180)|(lon > 180)|(lat < -90)|(lat > 90); swapped=(lon>=-90)&(lon<=90)&(lat>=-180)&(lat<=180)&(lat<0)
        valid=~(null|zero|oor); totals["null"]+=int(null.sum()); totals["zero"]+=int(zero.sum()); totals["out_of_range"]+=int(oor.sum()); totals["swapped_like"]+=int(swapped.sum()); totals["valid"]+=int(valid.sum())
        ix=np.flatnonzero(valid); pts=shapely.points(lon[ix],lat[ix]); pairs=tree.query(pts,predicate="intersects")
        counts=np.bincount(pairs[0],minlength=len(ix)) if pairs.size else np.zeros(len(ix),dtype=int)
        totals["unmatched"]+=int((counts==0).sum()); totals["matched_once"]+=int((counts==1).sum()); totals["multi"]+=int((counts>1).sum())
        # Boundary candidates: intersects but not strictly within any polygon.
        wpairs=tree.query(pts,predicate="within"); wcounts=np.bincount(wpairs[0],minlength=len(ix)) if wpairs.size else np.zeros(len(ix),dtype=int)
        totals["boundary"]+=int(((counts>0)&(wcounts==0)).sum())
        if len(samples)<10:
            for j in np.flatnonzero(counts==0)[:10-len(samples)]: samples.append({"longitude":lon[ix[j]],"latitude":lat[ix[j]],"reason":"valid_WGS84_but_no_zone_intersection"})
    return totals,samples


def main():
    init_dirs(); log=logger("profile_legacy_spatial")
    shp=next((REF/"taxi_zones_extracted").rglob("*.shp")); g=gpd.read_file(shp).to_crs(4326); tree=STRtree(g.geometry.to_numpy())
    reps=pd.read_csv(PROFILES/"representative_files.csv"); rows=[]; new_samples=[]
    an=pd.read_csv(ANOMALIES/"10_anomaly_summary.csv")
    for y,cols in COORDS.items():
        r=reps[reps.year==y].iloc[0]; pf=pq.ParquetFile(RAW/r.file_name)
        for role,(lon,lat) in {"pickup":cols[:2],"dropoff":cols[2:]}.items():
            totals,samps=scan_side(pf,lon,lat,tree); rows.append({"file":r.file_name,"year":y,"month":int(r.month),"role":role,**totals,"spatial_success_rate":totals["matched_once"]/totals["checked"]})
            invalid=totals["null"]+totals["zero"]+totals["out_of_range"]
            an.loc[len(an)]={"file":r.file_name,"year":y,"month":int(r.month),"rule_id":"DQ-SPATIAL-001","checked_rows":totals["checked"],"failed_rows":invalid,"failed_rate":invalid/totals["checked"],"severity":"Medium","recommendation":"Keep and flag; exclude invalid point from spatial join","condition":f"{role} coordinate null/zero/outside WGS84 physical range","description":"Invalid legacy coordinate"}
            an.loc[len(an)]={"file":r.file_name,"year":y,"month":int(r.month),"rule_id":"DQ-SPATIAL-002","checked_rows":totals["valid"],"failed_rows":totals["unmatched"],"failed_rate":totals["unmatched"]/totals["valid"] if totals["valid"] else 0,"severity":"High","recommendation":"Retain coordinates; map to Unknown only under approved rule","condition":"valid point does not intersect taxi-zone polygon in EPSG:4326","description":"Spatial join unmatched"}
            for s in samps: new_samples.append({"file":r.file_name,"year":y,"month":int(r.month),"rule_id":"DQ-SPATIAL-002","sample_json":json.dumps({"role":role,**s})})
            log.info("Spatial %s %s %s",r.file_name,role,totals)
    pd.DataFrame(rows).to_csv(ANOMALIES/"legacy_spatial_summary.csv",index=False,encoding="utf-8-sig"); an.to_csv(ANOMALIES/"10_anomaly_summary.csv",index=False,encoding="utf-8-sig")
    old=pq.read_table(ANOMALIES/"11_anomaly_samples.parquet").to_pandas(); allsamples=pd.concat([old,pd.DataFrame(new_samples)],ignore_index=True); pq.write_table(pa.Table.from_pandas(allsamples,preserve_index=False),ANOMALIES/"11_anomaly_samples.parquet",compression="zstd")


if __name__=="__main__": main()
