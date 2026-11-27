"""Source-to-STAGED QA benchmark, optional slim schema, with reader plans and disk samples.

Writes only UUID schemas. No live control calls; all module database helpers are
explicitly routed to QA schemas. Mage scheduler is not simulated by this harness.
"""
import argparse
import re
import json
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text
from postgres_stage_qa import StageQA
from utils.parquet_utils import calculate_file_sha256,inspect_parquet
from utils.workspace import artifact_bytes
import utils.atomic_stage as atomic
import transformers.load_source_files as source
import transformers.canonicalize_schema as canonical
import transformers.standardize_and_validate as standardize
import transformers.verify_stage as verify
import data_exporters.write_technical_quarantine as quarantine
import data_exporters.load_stage as exporter


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--qa',action='store_true',required=True)
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--rows',type=int,default=0)
    p.add_argument('--month',default='2024-05')
    p.add_argument('--storage',choices=['wide','slim'],default='wide')
    p.add_argument('--prepare',choices=['disk','streaming'],default='streaming')
    p.add_argument('--indexes',choices=['keep','drop-sha'],default='keep')
    p.add_argument('--batch-size',type=int,default=50000)
    p.add_argument('--work-root',type=Path,default=Path('/tmp/mage_m45_work'))
    args=p.parse_args()
    if args.rows<0 or args.batch_size<=0: raise ValueError('Invalid scope')
    qa=StageQA()
    monitor_stop=threading.Event();monitor=None;snapshots=[];monitor_errors=[]
    args.work_root.mkdir(parents=True,exist_ok=True)
    original_load=atomic.load_direct;original_verify=atomic.verify_published
    try:
        qa.setup(storage_mode=args.storage)
        if args.indexes=='drop-sha':
            if args.storage!='wide': raise ValueError('Index experiment requires wide baseline')
            with qa.engine.begin() as conn:
                for name in ('ix_yellow_taxi_raw_file_sha','ix_yellow_taxi_canonical_file_sha'):
                    conn.execute(text(f'DROP INDEX {qa.stage}.{name}'))
        with tempfile.TemporaryDirectory(prefix='m45_',dir=args.work_root) as tmp:
            root=Path(tmp);path=args.source
            if args.rows:
                # Identical, bounded sample for each variant; preparation excluded from timed ingestion.
                path=root/args.source.name
                count=0
                with pq.ParquetWriter(path,pq.ParquetFile(args.source).schema_arrow) as writer:
                    for rb in pq.ParquetFile(args.source).iter_batches(batch_size=args.batch_size):
                        table=pa.Table.from_batches([rb]).slice(0,min(rb.num_rows,args.rows-count))
                        writer.write_table(table);count+=table.num_rows
                        if count>=args.rows:break
            work=root/'work'
            def snapshot():
                with qa.engine.connect() as conn:
                    relations=conn.execute(text("SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN (:stage,:dq) AND c.relkind='r'"),{'stage':qa.stage,'dq':qa.dq}).scalar()
                    try:
                        temp=conn.execute(text('SELECT coalesce(sum(size),0) FROM pg_ls_tmpdir()')).scalar()
                    except Exception:
                        temp=None
                    snapshots.append({'elapsed':time.perf_counter()-started,'relation_bytes':int(relations),
                                      'work_bytes':artifact_bytes(work),'cluster_temp_bytes':None if temp is None else int(temp)})
            def observe():
                while not monitor_stop.wait(1):
                    try:snapshot()
                    except Exception as e:monitor_errors.append(type(e).__name__)
            started=time.perf_counter()
            metadata=inspect_parquet(path);sha=calculate_file_sha256(path)
            registry=pd.read_csv('/home/src/data/catalog/yellow_taxi_schema_registry.csv',dtype=str)
            versions=dict(zip(registry.schema_fingerprint,registry.schema_version))
            version=versions.get(metadata['schema_fingerprint'])
            if version is None:raise ValueError('Unregistered source schema')
            batch,file=qa.file({'file_name':path.name,'source_year_month':args.month,
                'source_file_sha256':sha,'schema_fingerprint':metadata['schema_fingerprint'],
                'schema_version':version,'last_modified':'2024-07-18T18:35:19+00:00',
                'parquet_row_count':metadata['row_count'],'resolved_path':str(path)})
            context={'batch_id':batch,'files':[file]}
            def safe_execute(sql,params=None):
                return qa.execute(re.sub(r'\bcontrol\.',qa.control+'.',sql),params)
            def load(c):
                return original_load(c,engine=qa.engine,stage=qa.stage,dq=qa.dq,control=qa.control,heartbeat=safe_execute)
            def published(c):return original_verify(c,engine=qa.engine,stage=qa.stage,dq=qa.dq,control=qa.control)
            snapshot();monitor=threading.Thread(target=observe,daemon=True);monitor.start()
            with patch.object(source,'execute',safe_execute),patch.object(canonical,'execute',safe_execute),patch.object(verify,'execute',safe_execute),patch.object(atomic,'load_direct',side_effect=load),patch.object(atomic,'verify_published',side_effect=published):
                context=source.load_source_files(context,prepare_mode=args.prepare,stage_load_mode='direct',work_root=str(work),source_batch_size=args.batch_size)
                context=canonical.canonicalize_schema(context,work_root=str(work),canonical_batch_size=args.batch_size)
                context=standardize.standardize_and_validate(context)
                context=quarantine.write_technical_quarantine(context,stage_load_mode='direct')
                context=exporter.load_stage(context,stage_load_mode='direct',stage_storage_mode=args.storage)
                result=verify.verify_stage(context)
            elapsed=time.perf_counter()-started
            snapshot();monitor_stop.set();monitor.join()
            queries={
                'raw_aggregate':f'SELECT count(*),sum(fare_amount::numeric(12,2)),sum(tip_amount::numeric(12,2)),sum(total_amount::numeric(12,2)) FROM {qa.stage}.yellow_taxi_raw WHERE source_file_sha256=:sha',
                'canonical_aggregate':f'SELECT count(*),sum(fare_amount),sum(tip_amount),sum(total_amount) FROM {qa.stage}.yellow_taxi_canonical WHERE source_file_sha256=:sha',
                'fingerprint_join':f'SELECT count(*) FROM {qa.stage}.yellow_taxi_raw r JOIN {qa.stage}.yellow_taxi_canonical c USING(source_record_key) WHERE r.source_file_sha256=:sha AND c.file_version_id=:version',
                'raw_json_consumer':f'SELECT sum(octet_length(to_jsonb(r)::text)) FROM {qa.stage}.yellow_taxi_raw r WHERE source_file_sha256=:sha AND source_row_number<=100000',
            }
            consumer={};amounts={}
            with qa.engine.connect() as conn:
                for label,query in queries.items():
                    params={'sha':sha,'version':file['file_version_id']}
                    begin=time.perf_counter();values=conn.execute(text(query),params).one()
                    consumer[label]={'seconds':time.perf_counter()-begin,'values':[str(v) for v in values],
                        'plan':conn.execute(text('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query),params).scalar()}
                if result['rows_technical_quarantined']==0 and consumer['raw_aggregate']['values']!=consumer['canonical_aggregate']['values']:
                    raise ValueError('Amount or count parity failed')
                relation_sizes=[dict(row) for row in conn.execute(text('SELECT relname,pg_table_size(relid) heap_toast_bytes,pg_indexes_size(relid) index_bytes,pg_total_relation_size(relid) total_bytes FROM pg_stat_user_tables WHERE schemaname=:stage'),{'stage':qa.stage}).mappings()]
            print('MAGE_M45_RESULT '+json.dumps({'prepare':args.prepare,'storage':args.storage,'indexes':args.indexes,
                'rows':metadata['row_count'],'batch_size':args.batch_size,'source_bytes':path.stat().st_size,
                'seconds_source_to_staged':elapsed,'result':result,'consumer_queries':consumer,'relations':relation_sizes,
                'workspace_bytes':artifact_bytes(work),'disk_samples':snapshots,'monitor_errors':monitor_errors,
                'scope':'Validated source + control file + prepare + COPY + validation/receipt, one Python process; excludes Mage scheduler, QA DDL and post-load consumer queries. Temp samples are cluster-wide, once per second, not exact backend peak.'},default=str),flush=True)
    finally:
        monitor_stop.set()
        if monitor is not None:monitor.join()
        qa.close()


if __name__=='__main__':main()
