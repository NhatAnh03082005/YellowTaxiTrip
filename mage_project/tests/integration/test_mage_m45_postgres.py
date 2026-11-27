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
from postgres_stage_qa import StageQA
from unit import test_task_4_4_utilities as fixtures
from utils.atomic_stage import load_direct,verify_published
from utils.parquet_utils import calculate_file_sha256,inspect_parquet
from utils.stream_parts import prepare_manifest
from utils.workspace import batch_directory,artifact_bytes


@unittest.skipUnless(os.environ.get('MAGE_POSTGRES_QA')=='1','QA opt-in required')
class MageCluster3Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)

    def qa(self,mode):
        qa=StageQA();self.addCleanup(qa.close);qa.setup(storage_mode=mode)
        return qa

    def context(self,qa,version='yt_schema_v001',clean=False,modified='2024-06-01T00:00:00+00:00'):
        raw=fixtures.CanonicalizationTests()._base_rows()
        raw=raw.drop(columns=[c for c in raw if c.startswith('source_') or c in ('batch_id','schema_version','schema_fingerprint')])
        if clean: raw['tpep_pickup_datetime']=['2024-05-01 10:00:00','2024-05-01 11:00:00']
        if version=='yt_schema_v002':raw['cbd_congestion_fee']=[2.5,2.5]
        path=self.root/(qa.prefix+'.parquet');raw.to_parquet(path,index=False)
        meta=inspect_parquet(path)
        batch,f=qa.file({'file_name':path.name,'source_year_month':'2024-05',
                        'source_file_sha256':calculate_file_sha256(path),'schema_fingerprint':meta['schema_fingerprint'],
                        'schema_version':version,'last_modified':modified,
                        'parquet_row_count':len(raw),'resolved_path':str(path)})
        return prepare_manifest({'batch_id':batch,'files':[f]},1)

    def load(self,qa,context,mode='wide',fault=None):
        return load_direct({**context,'stage_storage_mode':mode},engine=qa.engine,
                           stage=qa.stage,dq=qa.dq,control=qa.control,heartbeat=qa.execute,fault=fault)

    def test_streaming_no_spool_and_rejects_recoverable(self):
        qa=self.qa('wide');context=self.context(qa)
        def fault(phase):
            if phase=='after_canonical':raise RuntimeError('fault')
        with self.assertRaisesRegex(RuntimeError,'fault'):self.load(qa,context,fault=fault)
        result=self.load(qa,context)
        verified=verify_published(result,engine=qa.engine,stage=qa.stage,dq=qa.dq,control=qa.control)
        self.assertEqual(verified['rows_raw'],2);self.assertEqual(verified['rows_technical_quarantined'],1)
        self.assertTrue(self.load(qa,context)['direct_reused'])
        self.assertFalse((self.root/'raw').exists())

    def test_slim_schema_evolution_views_constraints_and_originals(self):
        for version in ('yt_schema_v001','yt_schema_v002'):
            qa=self.qa('slim');context=self.context(qa,version)
            result=self.load(qa,context,'slim')
            self.assertEqual(result['metrics']['rows_raw'],2)
            with qa.engine.connect() as conn:
                keys=inspect(conn).get_foreign_keys('yellow_taxi_raw_data',schema=qa.stage)
                self.assertEqual(len(keys),1)
                cols=[c['name'] for c in inspect(conn).get_columns('yellow_taxi_raw_data',schema=qa.stage)]
                self.assertNotIn('source_file_sha256',cols)
                view=conn.execute(text(f'SELECT source_file_sha256,schema_version,source_file FROM {qa.stage}.yellow_taxi_raw')).first()
                self.assertEqual(view.source_file_sha256,context['files'][0]['source_file_sha256'])
                self.assertEqual(view.schema_version,version)
                payload=conn.execute(text(f'SELECT raw_payload FROM {qa.dq}.technical_quarantine')).scalar()
                self.assertEqual(payload['tpep_pickup_datetime'],'not-a-time')
                self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.yellow_taxi_raw_wide_backup')).scalar(),0)

    def test_metadata_is_checked_before_omitting_repeated_values(self):
        qa=self.qa('slim');context=self.context(qa,clean=True)
        from utils import stream_parts
        original=stream_parts.iter_parts
        def wrong(c):
            for raw,canonical,reject,counts in original(c):
                raw['source_year_month']='2020-01'
                yield raw,canonical,reject,counts
        with patch.object(stream_parts,'iter_parts',wrong),self.assertRaisesRegex(ValueError,'lineage mismatch'):
            self.load(qa,context,'slim')
        with qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.yellow_taxi_raw_data')).scalar(),0)

    def test_file_metadata_cannot_change_behind_compatibility_views(self):
        qa=self.qa('slim');context=self.context(qa,clean=True)
        self.load(qa,context,'slim')
        with self.assertRaisesRegex(Exception,'immutable'):
            with qa.engine.begin() as conn:
                conn.execute(text(f"UPDATE {qa.control}.source_file_version SET source_year_month='2020-01'"))
        with qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.stage_file_batch')).scalar(),1)
            fk=inspect(conn).get_foreign_keys('stage_file_batch',schema=qa.stage)
            self.assertEqual(len(fk),2)

    def test_batch_lineage_is_not_silently_reassigned(self):
        qa=self.qa('slim');context=self.context(qa,clean=True)
        from utils import stream_parts
        original=stream_parts.iter_parts
        def wrong(c):
            for raw,canonical,reject,counts in original(c):
                canonical['batch_id']=999
                yield raw,canonical,reject,counts
        with patch.object(stream_parts,'iter_parts',wrong),self.assertRaisesRegex(ValueError,'batch_id'):
            self.load(qa,context,'slim')
        with qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.stage_file_batch')).scalar(),0)

    def test_missing_slim_migration_does_not_create_an_active_batch(self):
        from data_loaders.get_extraction_window import get_extraction_window
        with patch('data_loaders.get_extraction_window.fetch_one',return_value={'relation':None}) as query,self.assertRaisesRegex(RuntimeError,'no batch'):
            get_extraction_window(stage_storage_mode='slim',stage_load_mode='direct')
        self.assertEqual(query.call_count,1)
        self.assertNotIn('start_batch',query.call_args.args[0])

    def test_source_changed_rolls_back_whole_attempt(self):
        qa=self.qa('wide');context=self.context(qa)
        def fault(phase):
            if phase=='after_reject':
                path=Path(context['files'][0]['resolved_path'])
                path.touch()
        with self.assertRaisesRegex(ValueError,'Source changed'):
            self.load(qa,context,fault=fault)
        with qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.yellow_taxi_raw')).scalar(),0)

    def test_streaming_manifest_blocks_are_json_and_have_no_intermediate_io(self):
        qa=self.qa('wide');context=self.context(qa,clean=True)
        from transformers.load_source_files import load_source_files
        from transformers.canonicalize_schema import canonicalize_schema
        from transformers.standardize_and_validate import standardize_and_validate
        import json
        loaded=load_source_files(context,prepare_mode='streaming',stage_load_mode='direct',work_root=str(self.root/'work'))
        canonical=canonicalize_schema(loaded,work_root=str(self.root/'work'))
        prepared=standardize_and_validate(canonical)
        self.assertTrue(json.dumps(prepared))
        self.assertEqual(artifact_bytes(self.root/'work'),0)
        with self.assertRaisesRegex(ValueError,'direct'):
            load_source_files(context,prepare_mode='streaming',stage_load_mode='upsert')

    def test_workspace_guard_and_empty_reject_files(self):
        with self.assertRaises(ValueError):batch_directory(self.root,0)
        outside=self.root/'outside';outside.mkdir()
        root=self.root/'work';root.mkdir()
        (root/'1').symlink_to(outside,target_is_directory=True)
        with self.assertRaises(ValueError):batch_directory(root,1)
        qa=self.qa('wide');context=self.context(qa,clean=True)
        from transformers.load_source_files import load_source_files
        from transformers.canonicalize_schema import canonicalize_schema
        (root/'1').unlink()
        loaded=load_source_files(context,work_root=str(root),source_batch_size=2,prepare_mode='disk')
        canonical=canonicalize_schema(loaded,work_root=str(root),canonical_batch_size=1)
        self.assertTrue(all(c['reject_path']=='' for c in canonical['chunks']))
        self.assertEqual(list((root/'1'/'technical_reject').glob('*.parquet')),[])

    def test_slim_migration_refuses_hidden_view_dependencies(self):
        qa=self.qa('wide')
        qa.execute(f'CREATE VIEW {qa.audit}.raw_reader AS SELECT source_record_key FROM {qa.stage}.yellow_taxi_raw')
        with self.assertRaisesRegex(Exception,'Dependent views'):
            qa.run_migration('009_slim_stage_optin.sql')
        with qa.engine.connect() as conn:
            self.assertIsNone(inspect(conn).get_view_names(schema=qa.stage) or None)
            self.assertIsNone(conn.execute(text(f"SELECT to_regclass('{qa.stage}.stage_storage_contract')")).scalar())

    def test_empty_slim_schema_has_fail_closed_rollback(self):
        qa=self.qa('slim')
        qa.run_migration('009_slim_stage_rollback.sql')
        with qa.engine.connect() as conn:
            self.assertIn('yellow_taxi_raw',inspect(conn).get_table_names(schema=qa.stage))
            self.assertNotIn('yellow_taxi_raw_data',inspect(conn).get_table_names(schema=qa.stage))
            self.assertIsNone(conn.execute(text(f"SELECT to_regclass('{qa.stage}.stage_storage_contract')")).scalar())

    def test_deferred_slim_indexes_rollback_and_rebuild(self):
        qa=self.qa('slim');context=self.context(qa,clean=True)
        def fault(phase):
            if phase=='after_slim_index_drop':raise RuntimeError('index fault')
        with self.assertRaisesRegex(RuntimeError,'index fault'):
            self.load(qa,context,'slim',fault=fault)
        with qa.engine.connect() as conn:
            self.assertEqual(len(inspect(conn).get_indexes('yellow_taxi_raw_data',schema=qa.stage)),1)
            self.assertIsNotNone(inspect(conn).get_pk_constraint('yellow_taxi_raw_data',schema=qa.stage)['constrained_columns'])
        self.load(qa,context,'slim')
        with qa.engine.connect() as conn:
            self.assertEqual(len(inspect(conn).get_indexes('yellow_taxi_canonical_data',schema=qa.stage)),1)
            self.assertIsNotNone(inspect(conn).get_pk_constraint('yellow_taxi_canonical_data',schema=qa.stage)['constrained_columns'])

    def test_slim_v2_one_physical_copy_preserves_raw_canonical_and_quarantine(self):
        qa=self.qa('slim-v2');context=self.context(qa)
        result=self.load(qa,context,'slim')
        self.assertEqual(result['metrics']['rows_raw'],2)
        self.assertEqual(result['metrics']['rows_canonical'],1)
        self.assertEqual(result['metrics']['rows_technical_quarantined'],1)
        with qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT storage_mode FROM {qa.stage}.stage_storage_contract')).scalar(),'slim-v2')
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.yellow_taxi_trip_data')).scalar(),2)
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.yellow_taxi_raw')).scalar(),2)
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.yellow_taxi_canonical')).scalar(),1)
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.dq}.technical_quarantine')).scalar(),1)
            payload=conn.execute(text(f'SELECT raw_payload FROM {qa.dq}.technical_quarantine')).scalar()
            self.assertEqual(payload['tpep_pickup_datetime'],'not-a-time')
            columns={c['name'] for c in inspect(conn).get_columns('yellow_taxi_trip_data',schema=qa.stage)}
            self.assertNotIn('source_file',columns)
            self.assertNotIn('source_file_sha256',columns)
            self.assertIsNotNone(inspect(conn).get_pk_constraint('yellow_taxi_trip_data',schema=qa.stage)['constrained_columns'])
            self.assertEqual(len(inspect(conn).get_indexes('yellow_taxi_trip_data',schema=qa.stage)),1)

    def test_slim_v2_fault_rolls_back_unified_copy_and_metadata(self):
        qa=self.qa('slim-v2');context=self.context(qa,clean=True)
        def fault(phase):
            if phase=='after_unified':raise RuntimeError('unified fault')
        with self.assertRaisesRegex(RuntimeError,'unified fault'):
            self.load(qa,context,'slim',fault=fault)
        with qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.yellow_taxi_trip_data')).scalar(),0)
            self.assertEqual(conn.execute(text(f'SELECT count(*) FROM {qa.stage}.stage_file_batch')).scalar(),0)
            self.assertIsNotNone(inspect(conn).get_pk_constraint('yellow_taxi_trip_data',schema=qa.stage)['constrained_columns'])

    def test_empty_slim_v2_can_roll_back_to_slim_v1(self):
        qa=self.qa('slim-v2')
        qa.run_migration('010_slim_v2_rollback.sql')
        with qa.engine.connect() as conn:
            self.assertEqual(conn.execute(text(f'SELECT storage_mode FROM {qa.stage}.stage_storage_contract')).scalar(),'slim-v1')
            self.assertIsNone(conn.execute(text(f"SELECT to_regclass('{qa.stage}.yellow_taxi_trip_data')")).scalar())
            self.assertIn('yellow_taxi_raw_data',inspect(conn).get_table_names(schema=qa.stage))
