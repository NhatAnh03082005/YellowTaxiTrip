from __future__ import annotations

import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

from common import PROFILES, REF, init_dirs, logger


def main():
    init_dirs(); log=logger("profile_shapefile")
    z=REF/"taxi_zones.zip"; dest=REF/"taxi_zones_extracted"
    if not any(dest.rglob("*.shp")):
        with zipfile.ZipFile(z) as f: f.extractall(dest)
    shp=next(dest.rglob("*.shp")); g=gpd.read_file(shp); csv=pd.read_csv(REF/"taxi+_zone_lookup.csv")
    loc=next(c for c in g.columns if c.lower()=="locationid")
    components=[]
    for p in sorted(shp.parent.iterdir()): components.append(dict(metric="component",value=p.name,detail=p.suffix.lower(),count=p.stat().st_size))
    required={".shp",".shx",".dbf"}; present={p.suffix.lower() for p in shp.parent.iterdir()}
    invalid=~g.geometry.is_valid; empty=g.geometry.is_empty; null=g.geometry.isna(); dupgeom=g.geometry.to_wkb().duplicated(False)
    bounds=g.total_bounds
    metrics=[
        ("required_components_present",required.issubset(present),"Required: .shp,.shx,.dbf"),("feature_count",len(g),""),("geometry_types","|".join(sorted(g.geom_type.unique())),""),("crs",str(g.crs),""),("crs_units","|".join(ax.unit_name or "" for ax in g.crs.axis_info) if g.crs else "unknown",""),("bounding_box",",".join(map(str,bounds)),"minx,miny,maxx,maxy"),("attribute_columns","|".join(g.columns),""),("LocationID_dtype",str(g[loc].dtype),""),("LocationID_unique",g[loc].is_unique,""),("geometry_null",int(null.sum()),""),("geometry_empty",int(empty.sum()),""),("geometry_invalid",int(invalid.sum()),"includes self-intersection or other OGC invalidity"),("geometry_duplicate",int(dupgeom.sum()),"exact WKB duplicates"),("LocationID_duplicate",int(g[loc].duplicated(False).sum()),""),("needs_EPSG4326_for_web",str(g.crs).upper()!="EPSG:4326","reproject derivative only; preserve source")]
    prof=components+[dict(metric=m,value=v,detail=d,count="") for m,v,d in metrics]
    pd.DataFrame(prof).to_csv(PROFILES/"07_shapefile_profile.csv",index=False,encoding="utf-8-sig")
    attrs=g.drop(columns="geometry").rename(columns={loc:"LocationID_shp",next(c for c in g.columns if c.lower()=="borough"):"borough_shp",next(c for c in g.columns if c.lower()=="zone"):"zone_shp"})
    left=csv.merge(attrs,left_on="LocationID",right_on="LocationID_shp",how="outer",indicator=True)
    bshp="borough_shp"; zshp="zone_shp"
    comp=[]
    for _,r in left.iterrows():
        issues=[]
        if r._merge=="left_only": issues.append("CSV_ONLY")
        if r._merge=="right_only": issues.append("SHP_ONLY")
        if bshp and pd.notna(r.get("Borough")) and pd.notna(r.get(bshp)) and str(r.Borough).strip()!=str(r.get(bshp)).strip(): issues.append("BOROUGH_MISMATCH")
        if zshp and pd.notna(r.get("Zone")) and pd.notna(r.get(zshp)) and str(r.Zone).strip()!=str(r.get(zshp)).strip(): issues.append("ZONE_MISMATCH")
        comp.append(dict(LocationID_csv=r.get("LocationID",""),LocationID_shp=r.get("LocationID_shp",""),match_status=r._merge,borough_csv=r.get("Borough",""),borough_shp=r.get(bshp,""),zone_csv=r.get("Zone",""),zone_shp=r.get(zshp,""),issues="|".join(issues)))
    pd.DataFrame(comp).to_csv(PROFILES/"08_csv_shapefile_comparison.csv",index=False,encoding="utf-8-sig")
    log.info("Shapefile features=%s invalid=%s compare_rows=%s",len(g),invalid.sum(),len(comp))


if __name__ == "__main__": main()
