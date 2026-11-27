"""Actual Mage execute_sync + unmodified dbt models in a dedicated temporary database.

No production profiles/secrets are read. Connection values are inherited in env;
generated profiles reference env_var, and only CLI stdout/results are exported.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import threading
from pathlib import Path
from contextlib import closing

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import RealDictCursor
import yaml


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--qa',action='store_true',required=True)
    p.add_argument('--storage',choices=['wide','slim','slim-v2'],default='slim')
    p.add_argument('--prepare',choices=['disk','streaming'],default='streaming')
    p.add_argument('--rows',type=int,default=1000)
    p.add_argument('--month',default='2024-05')
    p.add_argument('--skip-dbt',action='store_true')
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    if not (0<=args.rows<=20000) or (args.rows==0 and not args.skip_dbt):raise ValueError('Full-source is allowed only with --skip-dbt')
    if args.month not in ('2024-05','2025-05'):raise ValueError('Representative QA month required')
    original_db=os.environ['POSTGRES_DB']
    database='mage_m45_qa_'+uuid.uuid4().hex[:12]
    admin=psycopg2.connect(host=os.environ['POSTGRES_HOST'],port=os.environ.get('POSTGRES_PORT','5432'),
        dbname=original_db,user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])
    admin.autocommit=True
    args.output.mkdir(parents=True,exist_ok=True)
    created=False;conn=None
    monitor_stop=threading.Event();monitor=None;snapshots=[];monitor_errors=[]
    code=Path(__file__).resolve().parents[1]
    sql_root=Path(os.environ['MAGE_QA_SQL_ROOT'])
    try:
        with admin.cursor() as cursor:cursor.execute('CREATE DATABASE '+database)
        created=True;os.environ['POSTGRES_DB']=database
        conn=psycopg2.connect(host=os.environ['POSTGRES_HOST'],port=os.environ.get('POSTGRES_PORT','5432'),
            dbname=database,user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])
        conn.autocommit=True
        with conn.cursor() as cursor:
            cursor.execute('CREATE EXTENSION IF NOT EXISTS postgis; CREATE EXTENSION IF NOT EXISTS pgcrypto;')
            for name in ['control/001_control_tables.sql','stage/001_stage_tables.sql','stage/002_stage_indexes.sql',
                         'control/003_control_functions.sql','migrations/007_replacement_recovery.sql']:
                cursor.execute('\n'.join(l for l in (sql_root/name).read_text(encoding='utf-8').splitlines() if not l.startswith('\\')))
            cursor.execute('ALTER TABLE stage.yellow_taxi_raw ADD COLUMN raw_payload jsonb')
            if args.storage in ('slim','slim-v2'):
                cursor.execute('\n'.join(l for l in (sql_root/'migrations/009_slim_stage_optin.sql').read_text(encoding='utf-8').splitlines() if not l.startswith('\\')))
            if args.storage=='slim-v2':
                cursor.execute('\n'.join(l for l in (sql_root/'migrations/010_slim_v2_unified_stage.sql').read_text(encoding='utf-8').splitlines() if not l.startswith('\\')))
        with tempfile.TemporaryDirectory(prefix='mage_m45_integration_') as tmp:
            root=Path(tmp);repo=root/'repo';repo.mkdir();data=root/'data';data.mkdir()
            for folder in ['utils','data_loaders','transformers','data_exporters']:
                shutil.copytree(code/folder,repo/folder,ignore=shutil.ignore_patterns('__pycache__','.gitkeep'))
            (repo/'metadata.yaml').write_text(yaml.safe_dump({'project_type':'standalone','help_improve_mage':False,
                'variables_dir':str(root/'variables'),'project_uuid':uuid.uuid4().hex}),encoding='utf-8')
            original_path=Path('/home/src/data/yellow_taxi/by_year_month')/args.month/('yellow_tripdata_'+args.month+'.parquet')
            original=pq.ParquetFile(original_path)
            path=data/original_path.name
            if args.rows:
                sample=pa.Table.from_batches([next(original.iter_batches(batch_size=args.rows))])
                pq.write_table(sample,path);row_count=sample.num_rows
            else:
                path=original_path;row_count=original.metadata.num_rows
            import hashlib
            parent_catalog=pd.read_csv('/home/src/data/catalog/yellow_taxi_2024_2025.csv',dtype=str)
            parent=parent_catalog.loc[parent_catalog['file_name']==path.name].iloc[0]
            sha=hashlib.sha256(path.read_bytes()).hexdigest() if args.rows else parent['sha256']
            data_root=data if args.rows else Path('/home/src/data')
            local_path=str(path.relative_to(data_root))
            catalog=root/'catalog.csv'
            pd.DataFrame([{'file_name':path.name,'source_year_month':args.month,'source_url':'qa://sample' if args.rows else parent['source_url'],
                'last_modified':parent['last_modified'],'sha256':sha,'local_path':local_path}]).to_csv(catalog,index=False)
            pipeline_id='qa_m45_pipeline'
            config=yaml.safe_load((code/'pipelines/pl_yellow_taxi_ingestion/metadata.yaml').read_text(encoding='utf-8'))
            config.update(uuid=pipeline_id,name=pipeline_id,variables_dir=str(root/'variables'),
                          run_pipeline_in_one_process=True,notification_config={})
            variables={**config['variables'],'run_type':'BACKFILL','catalog_path':str(catalog),
                'data_root':str(data_root),'schema_registry_path':'/home/src/data/catalog/yellow_taxi_schema_registry.csv',
                'source_year_month_from':args.month,'source_year_month_to':args.month,'work_root':str(root/'work'),
                'stage_load_mode':'direct','prepare_mode':args.prepare,
                'stage_storage_mode':'slim' if args.storage=='slim-v2' else args.storage}
            config['variables']=variables
            for block in config['blocks']:block.update(status='not_executed',all_upstream_blocks_executed=False)
            directory=repo/'pipelines'/pipeline_id;directory.mkdir(parents=True)
            (directory/'metadata.yaml').write_text(yaml.safe_dump(config,sort_keys=False),encoding='utf-8')
            sys.path.insert(0,str(repo))
            from mage_ai.data_preparation.models.pipeline import Pipeline
            pipeline=Pipeline.get(pipeline_id,repo_path=str(repo),use_repo_path=True)
            def sample_disk():
                from utils.workspace import artifact_bytes
                with closing(psycopg2.connect(host=os.environ['POSTGRES_HOST'],port=os.environ.get('POSTGRES_PORT','5432'),
                     dbname=database,user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])) as observation:
                    with observation.cursor() as cursor:
                        cursor.execute("SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('stage','dq') AND c.relkind='r'")
                        relation_bytes=int(cursor.fetchone()[0])
                        cursor.execute('SELECT coalesce(sum(size),0) FROM pg_ls_tmpdir()')
                        temp_bytes=int(cursor.fetchone()[0])
                snapshots.append({'seconds':time.perf_counter()-begin,'relation_bytes':relation_bytes,
                                  'work_bytes':artifact_bytes(root/'work'),'cluster_temp_bytes':temp_bytes})
            def observe():
                while not monitor_stop.wait(1):
                    try:sample_disk()
                    except Exception as e:monitor_errors.append(type(e).__name__)
            begin=time.perf_counter()
            if args.skip_dbt:
                monitor=threading.Thread(target=observe,daemon=True);monitor.start()
            pipeline.execute_sync(global_vars=variables,run_tests=True,update_status=False)
            mage_seconds=time.perf_counter()-begin
            if monitor is not None:
                sample_disk();monitor_stop.set();monitor.join()
            if args.skip_dbt:
                from utils.workspace import artifact_bytes
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute('SELECT batch_id,status,rows_raw,rows_canonical,rows_technical_quarantined,rows_fact_loaded FROM control.etl_batch')
                    state=cursor.fetchall()
                    cursor.execute("SELECT relname,pg_table_size(relid) heap_toast_bytes,pg_indexes_size(relid) index_bytes,pg_total_relation_size(relid) total_bytes FROM pg_stat_user_tables WHERE schemaname IN ('stage','dq')")
                    sizes=cursor.fetchall()
                output={'month':args.month,'storage':args.storage,'prepare':args.prepare,'rows':row_count,
                        'mage_execute_sync_seconds':mage_seconds,'state':state,'relations':sizes,
                        'workspace_bytes':artifact_bytes(root/'work'),'variables_bytes':artifact_bytes(root/'variables'),
                        'source_sha256':sha,'disk_samples':snapshots,'monitor_errors':monitor_errors,'scope':'Actual Mage execute_sync trip pipeline; includes checksum, control, source prepare, COPY and verification; no scheduler wait, DDS or reference init.'}
                (args.output/'result.json').write_text(json.dumps(output,default=str,indent=2),encoding='utf-8')
                print('MAGE_FULL_QA_PASS '+json.dumps(output,default=str),flush=True)
                return
            # The three reference sources use the same production functions in this QA database.
            from data_loaders.load_zone_lookup import load_zone_lookup
            from data_loaders.load_zone_geometry import load_zone_geometry
            from transformers.validate_zone_lookup import validate_zone_lookup
            from transformers.validate_zone_geometry import validate_zone_geometry
            from data_exporters.load_reference_stage import load_reference_stage
            lookup=validate_zone_lookup(load_zone_lookup())
            geometry=validate_zone_geometry(lookup,load_zone_geometry())
            load_reference_stage(lookup,geometry)
            project=Path('/tmp/mage_m45_dbt_project')
            profiles=root/'profiles';profiles.mkdir()
            settings={'type':'postgres','host':"{{ env_var('POSTGRES_HOST') }}",'port':int(os.environ.get('POSTGRES_PORT','5432')),
                'user':"{{ env_var('POSTGRES_USER') }}",'password':"{{ env_var('POSTGRES_PASSWORD') }}",
                'dbname':"{{ env_var('POSTGRES_DB') }}",'schema':'dds','threads':1}
            (profiles/'profiles.yml').write_text(yaml.safe_dump({'yellow_taxi_bi':{'target':'qa','outputs':{'qa':settings}}}),encoding='utf-8')
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute('SELECT batch_id,file_version_id FROM control.etl_file');identity=cursor.fetchone()
            variables={'dbt_minimal_mode':True,'load_batch_id':identity['batch_id'],'load_file_version_id':identity['file_version_id']}
            events=[]
            for label,command in [('seed',['seed']),('dds',['run','--select','+fact_taxi_trip','+business_quarantine','+duplicate_quarantine']),
                                  ('tests',['test','--select','source:stage','fact_taxi_trip','dim_date','dim_time','dim_zone','dim_vendor','dim_payment_type','dim_rate_code','--indirect-selection','cautious'])]:
                begin=time.perf_counter()
                result=subprocess.run(['dbt',*command,'--project-dir',str(project),'--profiles-dir',str(profiles),
                    '--target-path',str(root/'target'),'--log-path',str(root/'dbtlogs'),
                    '--no-send-anonymous-usage-stats','--vars',json.dumps(variables)],capture_output=True,text=True)
                (args.output/(label+'.log')).write_text(result.stdout+result.stderr,encoding='utf-8')
                events.append({'step':label,'seconds':time.perf_counter()-begin,'exit_code':result.returncode})
                if result.returncode:raise RuntimeError('QA dbt '+label+' failed; see output log')
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute('SELECT source_record_key::text,primary_disposition FROM dds.int_taxi_trip_classified ORDER BY source_record_key')
                dispositions=cursor.fetchall()
                cursor.execute('SELECT count(*) rows,sum(fare_amount) fare,sum(tip_amount) tip,sum(total_amount) total FROM dds.fact_taxi_trip')
                facts=cursor.fetchone()
                cursor.execute('SELECT batch_id,status,rows_raw,rows_canonical,rows_fact_loaded FROM control.etl_batch');batch_state=cursor.fetchall()
            output={'month':args.month,'storage':args.storage,'prepare':args.prepare,'rows':row_count,'mage_execute_sync_seconds':mage_seconds,
                'dbt':events,'facts':facts,'dispositions':dispositions,'batch_state':batch_state,
                'scope':'Real Mage execute_sync and dbt DDS fixture; no scheduler timing or monthly benchmark, no watermark commit.'}
            (args.output/'result.json').write_text(json.dumps(output,default=str,indent=2),encoding='utf-8')
            print('MAGE_DBT_QA_PASS '+json.dumps({'storage':args.storage,'rows':row_count,'mage_seconds':mage_seconds,'dbt':events,'facts':facts},default=str),flush=True)
    finally:
        monitor_stop.set()
        if monitor is not None:monitor.join()
        if conn is not None:conn.close()
        os.environ['POSTGRES_DB']=original_db
        if created:
            assert database.startswith('mage_m45_qa_') and len(database)==24
            with admin.cursor() as cursor:cursor.execute('DROP DATABASE '+database+' WITH (FORCE)')
        admin.close()


if __name__=='__main__':main()
