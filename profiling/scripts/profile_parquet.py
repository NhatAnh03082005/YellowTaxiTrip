from __future__ import annotations

import time

import pandas as pd
import pyarrow.parquet as pq

from common import PROFILES, RAW, init_dirs, logger


def main():
    init_dirs(); log = logger("profile_parquet")
    reps = pd.read_csv(PROFILES / "representative_files.csv")
    files=[]; schemas=[]
    for _, r in reps.iterrows():
        p = RAW / r.file_name; t0=time.perf_counter(); pf=pq.ParquetFile(p); md=pf.metadata; schema=pf.schema_arrow
        files.append(dict(file=r.file_name,year=r.year,month=r.month,url=r.url,size_bytes=p.stat().st_size,row_groups=md.num_row_groups,total_rows=md.num_rows,num_columns=md.num_columns,columns="|".join(schema.names),read_metadata_seconds=round(time.perf_counter()-t0,6),status="readable",selection_reason=r.selection_reason,sha256=r.sha256))
        for pos, field in enumerate(schema):
            mins=[]; maxs=[]; nulls=0; stats_groups=0; physical=set()
            for rg in range(md.num_row_groups):
                col=md.row_group(rg).column(pos); physical.add(col.physical_type)
                st=col.statistics
                if st:
                    stats_groups += 1; nulls += st.null_count or 0
                    if st.has_min_max: mins.append(st.min); maxs.append(st.max)
            def safe(vs, fn):
                if not vs: return ""
                try: return str(fn(vs))
                except Exception: return "|".join(map(str,vs[:3]))
            schemas.append(dict(file=r.file_name,year=r.year,month=r.month,column_order=pos+1,column_name=field.name,physical_type="|".join(sorted(physical)),logical_type=str(field.type),nullable=field.nullable,metadata_null_count=nulls,metadata_null_rate=(nulls/md.num_rows if md.num_rows else 0),row_groups_with_statistics=stats_groups,metadata_min=safe(mins,min),metadata_max=safe(maxs,max)))
        log.info("Profiled %s rows=%s cols=%s", p.name, md.num_rows, md.num_columns)
    pd.DataFrame(files).to_csv(PROFILES / "02_parquet_file_profile.csv",index=False,encoding="utf-8-sig")
    pd.DataFrame(schemas).to_csv(PROFILES / "03_schema_by_file.csv",index=False,encoding="utf-8-sig")


if __name__ == "__main__": main()
