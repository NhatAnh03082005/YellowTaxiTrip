"""QA replay with real FKs/control functions, bounded re-batching, no live writes."""
import argparse
import json
import tempfile
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from postgres_stage_qa import StageQA
from utils.atomic_stage import load_direct,verify_published,params_for,reader_for
from utils.stage_loader import StageWriter
from transformers.verify_stage import validate_file_stage


def parts(root,rows,batch_size,output):
    raws=sorted((root/'raw').glob('*.parquet'))
    canonical=sorted((root/'canonical').glob('*.parquet'))
    if not raws or len(raws)!=len(canonical): raise ValueError('Expected baseline 1:1 parts')
    pending_raw=pending_canonical=None
    total=0;index=0
    chunks=[]
    def save(raw,canonical):
        nonlocal index
        r=output/f'raw_{index:05d}.parquet';c=output/f'canonical_{index:05d}.parquet';q=output/f'reject_{index:05d}.parquet'
        raw.to_parquet(r,index=False);canonical.to_parquet(c,index=False);canonical.iloc[:0].to_parquet(q,index=False)
        chunks.append({'raw_path':str(r),'canonical_path':str(c),'reject_path':str(q),
                       'row_count':len(raw),'source_part_rows':len(raw),'canonical_rows':len(canonical),
                       'technical_reject_rows':0,'source_file':str(raw.iloc[0].source_file)})
        index+=1
    for r,c in zip(raws,canonical):
        raw=pd.read_parquet(r);canon=pd.read_parquet(c)
        if len(raw)!=len(canon): raise ValueError('Baseline profile supports zero rejects only')
        if rows: raw=raw.iloc[:max(0,rows-total)];canon=canon.iloc[:len(raw)]
        total+=len(raw)
        pending_raw=raw if pending_raw is None else pd.concat([pending_raw,raw],ignore_index=True)
        pending_canonical=canon if pending_canonical is None else pd.concat([pending_canonical,canon],ignore_index=True)
        while len(pending_raw)>=batch_size:
            save(pending_raw.iloc[:batch_size],pending_canonical.iloc[:batch_size])
            pending_raw=pending_raw.iloc[batch_size:];pending_canonical=pending_canonical.iloc[batch_size:]
        if rows and total>=rows: break
    if pending_raw is not None and len(pending_raw): save(pending_raw,pending_canonical)
    return chunks,total


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--qa',action='store_true',required=True)
    p.add_argument('--parts',type=Path,required=True)
    p.add_argument('--rows',type=int,default=0)
    p.add_argument('--batch-size',type=int,default=100000)
    p.add_argument('--mode',choices=['upsert','direct'],default='direct')
    p.add_argument('--serialization',choices=['arrow','pandas'],default='arrow')
    args=p.parse_args()
    if args.rows<0 or args.batch_size<=0: raise ValueError('Invalid size')
    qa=StageQA()
    try:
        qa.setup()
        with tempfile.TemporaryDirectory(prefix='mage_m23_parts_') as tmp:
            chunks,total=parts(args.parts,args.rows,args.batch_size,Path(tmp))
            first=pd.read_parquet(chunks[0]['raw_path']).iloc[0]
            batch,file=qa.file({'file_name':str(first.source_file),'source_year_month':str(first.source_year_month),
                                'source_file_sha256':str(first.source_file_sha256),'schema_fingerprint':str(first.schema_fingerprint),
                                'schema_version':str(first.schema_version),'parquet_row_count':total})
            # Baseline replay preserves original IDs only when equal to the fresh QA IDs.
            if batch!=int(first.batch_id) or file['file_version_id']!=int(first.file_version_id):
                raise ValueError('QA identities differ from baseline parts; refusing silent identity rewrite')
            context={'batch_id':batch,'files':[file],'chunks':chunks,
                     'metrics':{'rows_raw':total,'rows_canonical':total,'rows_technical_quarantined':0}}
            with qa.engine.connect() as conn:
                before=dict(conn.execute(text('SELECT temp_bytes,pg_database_size(current_database()) db_bytes FROM pg_stat_database WHERE datname=current_database()')).mappings().one())
            start=time.perf_counter()
            if args.mode=='direct':
                if args.serialization!='arrow': raise ValueError('Direct production serializer is arrow')
                result=load_direct(context,engine=qa.engine,stage=qa.stage,dq=qa.dq,control=qa.control,heartbeat=qa.execute)
                verify_published(result,engine=qa.engine,stage=qa.stage,dq=qa.dq,control=qa.control)
                counts=result['write_counts']
            else:
                counts={'raw':{},'canonical':{}}
                with StageWriter(serialization=args.serialization,engine=qa.engine) as writer:
                    for chunk in chunks:
                        for key,table,kind in [('raw_path','yellow_taxi_raw','raw'),('canonical_path','yellow_taxi_canonical','canonical')]:
                            frame=pd.read_parquet(chunk[key])
                            writer.write(frame,qa.stage,table,metrics=counts[kind])
                    validate_file_stage(params_for(context),chunks,reader=reader_for(writer.connection,qa.stage,qa.dq))
            elapsed=time.perf_counter()-start
            with qa.engine.connect() as conn:
                amounts={}
                for table in ['yellow_taxi_raw','yellow_taxi_canonical']:
                    amounts[table]=[str(v) for v in conn.execute(text(f'SELECT sum(fare_amount::numeric(12,2)),sum(tip_amount::numeric(12,2)),sum(total_amount::numeric(12,2)) FROM {qa.stage}.{table}')).one()]
                if amounts['yellow_taxi_raw']!=amounts['yellow_taxi_canonical']: raise ValueError('Amount parity failed')
                after=dict(conn.execute(text('SELECT temp_bytes,pg_database_size(current_database()) db_bytes FROM pg_stat_database WHERE datname=current_database()')).mappings().one())
                sizes=[dict(r) for r in conn.execute(text('SELECT relname,pg_total_relation_size(relid) bytes FROM pg_stat_user_tables WHERE schemaname=:schema'),{'schema':qa.stage}).mappings()]
            print('MAGE_M23_RESULT '+json.dumps({'mode':args.mode,'serialization':args.serialization,'batch_size':args.batch_size,
                'rows':total,'seconds_load_and_verify':elapsed,'counts':counts,'amounts':amounts,'before':before,'after':after,'relations':sizes,
                'scope':'QA local part replay; real FKs/control; excludes input re-batching, source transform, scheduler, final amount-query time'}),flush=True)
    finally:
        qa.close()


if __name__=='__main__': main()
