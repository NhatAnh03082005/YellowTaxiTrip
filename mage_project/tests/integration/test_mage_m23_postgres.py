import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pandas as pd
from sqlalchemy import text,inspect
from psycopg2.errors import ForeignKeyViolation
from postgres_stage_qa import StageQA
from utils.atomic_stage import load_direct,verify_published,state
from utils.canonical import canonicalize_dataframe,serialize_json_columns
from utils.parquet_utils import make_source_record_key
from utils.stage_loader import StageWriter
from unit import test_task_4_4_utilities as fixtures


@unittest.skipUnless(os.environ.get('MAGE_POSTGRES_QA')=='1','QA opt-in required')
class AtomicStageTests(unittest.TestCase):
    def setUp(self):
        self.qa=StageQA()
        self.addCleanup(self.qa.close)
        self.qa.setup()
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.options={'engine':self.qa.engine,'stage':self.qa.stage,'dq':self.qa.dq,
                      'control':self.qa.control,'heartbeat':self.qa.execute}

    def context(self,invalid=False):
        raw=fixtures.CanonicalizationTests()._base_rows()
        if invalid: raw['total_amount']='bad'
        batch,file=self.qa.file({'file_name':'file.parquet','source_year_month':'2024-05',
                                'source_file_sha256':'a'*64,'schema_fingerprint':'b'*64,
                                'schema_version':'yt_schema_v001','parquet_row_count':2})
        raw['batch_id']=batch;raw['file_version_id']=file['file_version_id']
        raw['source_record_key']=[make_source_record_key('a'*64,n) for n in (1,2)]
        canonical,reject=canonicalize_dataframe(raw)
        root=Path(self.tmp.name)
        for name,frame in [('raw',raw),('canonical',canonical),('reject',reject)]:
            serialize_json_columns(frame).to_parquet(root/(name+'.parquet'),index=False)
        return {'batch_id':batch,'files':[file],
                'chunks':[{'raw_path':str(root/'raw.parquet'),'canonical_path':str(root/'canonical.parquet'),
                           'reject_path':str(root/'reject.parquet'),'source_file':'file.parquet',
                           'row_count':2,'source_part_rows':2,'canonical_rows':len(canonical),'technical_reject_rows':len(reject)}],
                'metrics':{'rows_raw':2,'rows_canonical':len(canonical),'rows_technical_quarantined':len(reject)}}

    def verify(self,context):
        return verify_published(context,engine=self.qa.engine,stage=self.qa.stage,dq=self.qa.dq,control=self.qa.control)

    def test_atomic_visibility_retry_and_watermark(self):
        context=self.context()
        def observer(phase):
            if phase in ('after_raw','after_canonical','after_reject','before_commit'):
                with self.qa.engine.connect() as conn:
                    self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {self.qa.stage}.yellow_taxi_raw')).scalar(),0)
                    self.assertNotEqual(state(conn,context['batch_id'],self.qa.control)['batch_status'],'STAGED')
        result=load_direct(context,**self.options,fault=observer)
        self.assertEqual(self.verify(result)['rows_raw'],2)
        self.assertTrue(load_direct(context,**self.options)['direct_reused'])
        with self.qa.engine.connect() as conn:
            stamp=conn.execute(text(f'SELECT last_successful_extraction_time::text FROM {self.qa.control}.etl_watermark')).scalar()
        self.assertTrue(stamp.startswith('1970-01-01'))

    def test_faults_rollback_all_tables_and_retry(self):
        context=self.context()
        for failure in ('after_raw','after_canonical','after_reject','before_verify','before_commit'):
            def fault(phase):
                if phase==failure: raise RuntimeError('Injected '+phase)
            with self.assertRaisesRegex(RuntimeError,'Injected'):
                load_direct(context,**self.options,fault=fault)
            with self.qa.engine.connect() as conn:
                for schema,name in [(self.qa.stage,'yellow_taxi_raw'),(self.qa.stage,'yellow_taxi_canonical'),(self.qa.dq,'technical_quarantine')]:
                    self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {schema}.{name}')).scalar(),0)
                self.assertEqual(state(conn,context['batch_id'],self.qa.control)['batch_status'],'FAILED')
        self.assertEqual(self.verify(load_direct(context,**self.options))['rows_raw'],2)

    def test_commit_ack_loss_can_resume_without_reload(self):
        context=self.context()
        def fault(phase):
            if phase=='after_commit': raise RuntimeError('Lost ack')
        with self.assertRaisesRegex(RuntimeError,'Lost ack'):
            load_direct(context,**self.options,fault=fault)
        self.assertTrue(load_direct(context,**self.options)['direct_reused'])

    def test_missing_data_after_publish_fails_closed(self):
        result=load_direct(self.context(),**self.options)
        with self.qa.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE {self.qa.stage}.yellow_taxi_raw,{self.qa.stage}.yellow_taxi_canonical'))
        with self.assertRaisesRegex(ValueError,'missing'):
            self.verify(result)
        with self.assertRaises(ValueError): load_direct(result,**self.options)

    def test_dirty_target_lock_and_receipt_guards(self):
        context=self.context()
        with self.qa.engine.connect() as lock:
            name='yellow_taxi_staging_writer:'+self.qa.stage
            lock.execute(text('SELECT pg_advisory_lock(hashtext(:name))'),{'name':name})
            with self.assertRaisesRegex(RuntimeError,'Another'):
                load_direct(context,**self.options)
            lock.execute(text('SELECT pg_advisory_unlock(hashtext(:name))'),{'name':name})
        from utils.stage_loader import upsert_dataframe
        upsert_dataframe(pd.read_parquet(context['chunks'][0]['raw_path']),self.qa.stage,'yellow_taxi_raw')
        with self.assertRaisesRegex(ValueError,'not clean'):
            load_direct(context,**self.options)
        with self.qa.engine.begin() as conn:
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {self.qa.stage}.yellow_taxi_raw')).scalar(),2)

    def test_reject_all_and_constraints_retained(self):
        context=self.context(invalid=True)
        self.assertEqual(self.verify(load_direct(context,**self.options))['rows_technical_quarantined'],2)
        with self.qa.engine.connect() as conn:
            self.assertEqual(len(inspect(conn).get_foreign_keys('yellow_taxi_raw',schema=self.qa.stage)),2)

    def test_bad_partition_rolls_back_and_no_fallback(self):
        context=self.context()
        path=context['chunks'][0]['reject_path']
        frame=pd.read_parquet(path)
        frame['source_record_key']=make_source_record_key('a'*64,1)
        frame.to_parquet(path,index=False)
        with self.assertRaisesRegex(ValueError,'partition'):
            load_direct(context,**self.options)

    def test_arrow_and_pandas_equivalent_roundtrip(self):
        frame=pd.DataFrame({'source_record_key':[str(i) for i in range(7)],
                            'value':[None,'',r'\N','a,"b"\nnext',r'c:\folder','café','東京']})
        with self.qa.engine.begin() as conn:
            for name in ('csv_arrow','csv_pandas'):
                conn.execute(text(f'CREATE TABLE {self.qa.stage}.{name}(source_record_key text UNIQUE NOT NULL,value text)'))
        for fmt in ('arrow','pandas'):
            with StageWriter(serialization=fmt,engine=self.qa.engine) as writer:
                writer.write(frame,self.qa.stage,'csv_'+fmt)
        with self.qa.engine.connect() as conn:
            rows=[conn.execute(text(f'SELECT * FROM {self.qa.stage}.csv_{fmt} ORDER BY source_record_key')).all() for fmt in ('arrow','pandas')]
        self.assertEqual(rows[0],rows[1])

    def test_direct_exporter_defers_reject_and_uses_published_receipt(self):
        from data_exporters.write_technical_quarantine import write_technical_quarantine
        from data_exporters.load_stage import load_stage
        from transformers.verify_stage import verify_stage
        context=self.context()
        deferred=write_technical_quarantine(context,stage_load_mode='direct')
        self.assertTrue(deferred['quarantine_deferred'])
        original=load_direct
        with patch('utils.atomic_stage.load_direct',side_effect=lambda c:original(c,**self.options)):
            published=load_stage(deferred,stage_load_mode='direct')
        with patch('utils.atomic_stage.verify_published',side_effect=self.verify):
            verified=verify_stage(published)
        self.assertEqual(verified['status'],'STAGED')
        with self.assertRaisesRegex(ValueError,'Deferred'):
            load_stage(deferred,stage_load_mode='upsert')
        with self.assertRaisesRegex(ValueError,'Deferred'):
            write_technical_quarantine(deferred,stage_load_mode='upsert')

    def test_schema_cache_and_source_dataframe_not_modified(self):
        from utils.stage_loader import normalize_copy_frame
        frame=pd.DataFrame({'passenger_count':[1.0,None]})
        old=frame.copy(deep=True)
        normalize_copy_frame(frame,{'passenger_count':'SMALLINT'})
        pd.testing.assert_frame_equal(frame,old)
        with StageWriter(engine=self.qa.engine) as writer:
            first=writer.metadata(self.qa.stage,'yellow_taxi_raw')
            with patch('utils.stage_loader.inspect',side_effect=RuntimeError('Unexpected second inspection')):
                self.assertIs(writer.metadata(self.qa.stage,'yellow_taxi_raw'),first)

    def test_upsert_quarantine_exporter_reuses_session_and_counts_retry(self):
        import data_exporters.write_technical_quarantine as exporter
        import utils.stage_loader as loader
        context=self.context()
        original=loader.upsert_dataframe
        def routed(frame,schema,table,**kwargs):
            return original(frame,self.qa.dq,table,**kwargs)
        with patch.object(exporter,'reload',lambda module:module),patch.object(exporter,'require_tables'),patch.object(loader,'upsert_dataframe',routed):
            first=exporter.write_technical_quarantine(context)
            second=exporter.write_technical_quarantine(context)
        self.assertEqual(first['technical_quarantine_write_counts']['inserted'],1)
        self.assertEqual(second['technical_quarantine_write_counts']['conflicted'],1)

    def test_fk_or_unique_failure_never_partially_publishes(self):
        context=self.context()
        path=context['chunks'][0]['canonical_path']
        frame=pd.read_parquet(path);frame['file_version_id']=999999
        frame.to_parquet(path,index=False)
        with self.assertRaises(ForeignKeyViolation): load_direct(context,**self.options)
        with self.qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {self.qa.stage}.yellow_taxi_raw')).scalar(),0)
            self.assertEqual(state(conn,context['batch_id'],self.qa.control)['batch_status'],'FAILED')

    def test_receipt_mismatch_and_dds_running_never_reload(self):
        context=self.context()
        published=load_direct(context,**self.options)
        changed={**context,'metrics':{**context['metrics'],'rows_raw':3}}
        with self.assertRaisesRegex(ValueError,'receipt'):
            load_direct(changed,**self.options)
        with self.qa.engine.begin() as conn:
            conn.execute(text(f"SELECT {self.qa.control}.transition_batch(:batch,'DBT_RUNNING')"),{'batch':context['batch_id']})
        with self.assertRaisesRegex(ValueError,'not writable'):
            load_direct(published,**self.options)
        with self.qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {self.qa.stage}.yellow_taxi_raw')).scalar(),2)
