"""Isolated schemas using actual repository control/stage DDL and functions."""
import os
import uuid
from pathlib import Path

from sqlalchemy import text
from utils.stage_loader import postgres_engine


class StageQA:
    def __init__(self):
        self.prefix='mage_m23_qa_'+uuid.uuid4().hex[:10]
        self.stage=self.prefix+'_stage'
        self.dq=self.prefix+'_dq'
        self.control=self.prefix+'_control'
        self.audit=self.prefix+'_audit'
        self.engine=postgres_engine()

    def setup(self, storage_mode='wide'):
        root=Path(os.environ.get('MAGE_QA_SQL_ROOT',str(Path(__file__).resolve().parents[2]/'sql')))
        for name in ['control/001_control_tables.sql','stage/001_stage_tables.sql',
                     'stage/002_stage_indexes.sql','control/003_control_functions.sql']:
            script='\n'.join(line for line in (root/name).read_text(encoding='utf-8').splitlines() if not line.startswith('\\'))
            import re
            for src,dst in [('control',self.control),('stage',self.stage),('dq',self.dq),('audit',self.audit)]:
                script=re.sub(r'\b'+src+r'\.',dst+'.',script)
                script=re.sub(r'(?<=EXISTS )'+src+r';',dst+';',script)
            with self.engine.begin() as conn:
                with conn.connection.cursor() as cursor:
                    cursor.execute(script)
        with self.engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE {self.stage}.yellow_taxi_raw ADD COLUMN IF NOT EXISTS raw_payload jsonb'))
        if storage_mode in ('slim','slim-v2'):
            self.run_migration('009_slim_stage_optin.sql')
        if storage_mode=='slim-v2':
            self.run_migration('010_slim_v2_unified_stage.sql')
        return self

    def run_migration(self,name):
        root=Path(os.environ.get('MAGE_QA_SQL_ROOT',str(Path(__file__).resolve().parents[2]/'sql')))
        script='\n'.join(line for line in (root/'migrations'/name).read_text(encoding='utf-8').splitlines() if not line.startswith('\\'))
        import re
        for src,dst in [('control',self.control),('stage',self.stage),('dq',self.dq),('audit',self.audit)]:
            script=re.sub(r'\b'+src+r'\.',dst+'.',script)
            script=re.sub(r'(?<=EXISTS )'+src+r';',dst+';',script)
            script=script.replace("'"+src+"'","'"+dst+"'")
        with self.engine.begin() as conn:
            with conn.connection.cursor() as cursor: cursor.execute(script)

    def close(self):
        with self.engine.begin() as conn:
            for schema in (self.stage,self.dq,self.control,self.audit):
                assert schema.startswith(self.prefix+'_') and self.prefix.startswith('mage_m23_qa_')
                conn.execute(text('DROP SCHEMA IF EXISTS '+schema+' CASCADE'))
        self.engine.dispose()

    def execute(self,query,params=None):
        with self.engine.begin() as conn: conn.exec_driver_sql(query,params or {})

    def file(self, metadata):
        with self.engine.begin() as conn:
            batch=conn.execute(text(f"SELECT batch_id FROM {self.control}.start_batch('qa_m23','taxi','TEST','1970-01-01'::timestamptz)")).scalar()
            file_id=conn.execute(text(f"SELECT {self.control}.discover_file(:batch,:file,:month,NULL,CAST(:modified AS timestamptz))"),
                                 {'batch':batch,'file':metadata['file_name'],'month':metadata['source_year_month'],'modified':metadata.get('last_modified')}).scalar()
            conn.execute(text(f'SELECT {self.control}.validate_file(:id,100,:sha,:fp,:schema)'),
                         {'id':file_id,'sha':metadata['source_file_sha256'],'fp':metadata['schema_fingerprint'],'schema':metadata['schema_version']})
            version=conn.execute(text(f'SELECT file_version_id FROM {self.control}.etl_file WHERE etl_file_id=:id'),{'id':file_id}).scalar()
        return batch,{**metadata,'batch_id':batch,'etl_file_id':file_id,'file_version_id':version}
