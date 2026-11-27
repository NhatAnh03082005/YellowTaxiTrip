"""Opt-in real PostgreSQL tests. Writes only UUID-named QA schemas; never live tables."""
import os
import tempfile
import unittest
import uuid
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import pandas as pd
from sqlalchemy import text
from utils import stage_loader as loader
from utils.canonical import canonicalize_dataframe
from utils.parquet_utils import make_source_record_key
from transformers.verify_stage import validate_file_stage
from transformers.load_source_files import load_source_files
from transformers.canonicalize_schema import canonicalize_schema
from transformers.standardize_and_validate import standardize_and_validate
try:
    from mage_project.tests.unit import test_task_4_4_utilities as fixtures
except ModuleNotFoundError:
    from unit import test_task_4_4_utilities as fixtures


@unittest.skipUnless(os.environ.get('MAGE_POSTGRES_QA') == '1', 'Explicit QA opt-in required')
class MagePostgresQA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = 'mage_m01_qa_' + uuid.uuid4().hex[:12]
        cls.engine = loader.postgres_engine()
        with cls.engine.begin() as conn:
            conn.execute(text('CREATE SCHEMA '+cls.schema))
            for name, source in [('yellow_taxi_raw','stage'),('yellow_taxi_canonical','stage'),('technical_quarantine','dq')]:
                conn.execute(text(f'CREATE UNLOGGED TABLE {cls.schema}.{name} (LIKE {source}.{name} INCLUDING ALL)'))
            conn.execute(text(f'ALTER TABLE {cls.schema}.yellow_taxi_raw ADD COLUMN IF NOT EXISTS raw_payload jsonb'))
            conn.execute(text(f'CREATE TABLE {cls.schema}.copy_strings (source_record_key text UNIQUE NOT NULL,value text)'))

    @classmethod
    def tearDownClass(cls):
        assert cls.schema.startswith('mage_m01_qa_') and len(cls.schema) == 24
        with cls.engine.begin() as conn:
            conn.execute(text('DROP SCHEMA '+cls.schema+' CASCADE'))
        cls.engine.dispose()

    def setUp(self):
        with self.engine.begin() as conn:
            for name in ['yellow_taxi_raw','yellow_taxi_canonical','technical_quarantine','copy_strings']:
                conn.execute(text(f'TRUNCATE {self.schema}.{name}'))

    def rows(self, version='yt_schema_v001'):
        raw = fixtures.CanonicalizationTests()._base_rows()
        raw['file_version_id'] = 101
        raw['schema_version'] = version
        raw['source_record_key'] = [make_source_record_key('a'*64,n) for n in (1,2)]
        if version == 'yt_schema_v002': raw['cbd_congestion_fee'] = [2.5,2.5]
        return raw

    def query_one(self, query, params):
        query = query.replace('stage.yellow_',self.schema+'.yellow_').replace('dq.technical_quarantine',self.schema+'.technical_quarantine')
        with self.engine.begin() as conn:
            return dict(conn.execute(text(__import__('re').sub(r'%\((\w+)\)s',r':\1',query)), params).mappings().one())

    def load_and_validate(self, raw):
        accepted,rejected = canonicalize_dataframe(raw)
        for frame,table in [(raw,'yellow_taxi_raw'),(accepted,'yellow_taxi_canonical'),(rejected,'technical_quarantine')]:
            loader.upsert_dataframe(frame,self.schema,table)
        params={'batch':10,'version':101,'sha':'a'*64,'file':'file.parquet','expected':len(raw),'schema':str(raw.iloc[0].schema_version),'fingerprint':'b'*64,'month':'2024-05'}
        chunks=[{'source_file':'file.parquet','row_count':len(raw),'canonical_rows':len(accepted),'technical_reject_rows':len(rejected)}]
        with patch('transformers.verify_stage.fetch_one',self.query_one):
            validate_file_stage(params,chunks)
        return params,chunks

    def test_two_schemas_and_optional_cast_original(self):
        for version in ('yt_schema_v001','yt_schema_v002'):
            self.setUp()
            raw=self.rows(version);raw.loc[0,'passenger_count']=1.5
            self.load_and_validate(raw)
            with self.engine.begin() as conn:
                payload=conn.execute(text(f'SELECT raw_payload FROM {self.schema}.yellow_taxi_canonical')).scalar()
            self.assertEqual(payload['passenger_count'],1.5)
            with self.engine.begin() as conn:
                amount=conn.execute(text(f'SELECT sum(total_amount) FROM {self.schema}.yellow_taxi_canonical')).scalar()
                raw_amount=conn.execute(text(f'SELECT sum(total_amount::numeric(12,2)) FROM {self.schema}.yellow_taxi_raw')).scalar()
                rejected_amount=conn.execute(text(f"SELECT sum((raw_payload->>'total_amount')::numeric(12,2)) FROM {self.schema}.technical_quarantine")).scalar()
            self.assertEqual(raw_amount,amount+rejected_amount)

    def test_count_balanced_but_missing_rows_fails(self):
        params,chunks=self.load_and_validate(self.rows())
        with self.engine.begin() as conn:
            conn.execute(text(f'DELETE FROM {self.schema}.yellow_taxi_raw WHERE source_row_number=1'))
            conn.execute(text(f'DELETE FROM {self.schema}.yellow_taxi_canonical'))
        with patch('transformers.verify_stage.fetch_one',self.query_one),self.assertRaises(ValueError):
            validate_file_stage(params,chunks)

    def test_wrong_partition_even_with_correct_counts_fails(self):
        params,chunks=self.load_and_validate(self.rows())
        with self.engine.begin() as conn:
            conn.execute(text(f"UPDATE {self.schema}.technical_quarantine SET source_record_key=:key"),{'key':make_source_record_key('a'*64,1)})
        with patch('transformers.verify_stage.fetch_one',self.query_one),self.assertRaises(ValueError):
            validate_file_stage(params,chunks)

    def test_wrong_scope_and_footer_fail(self):
        params,chunks=self.load_and_validate(self.rows())
        with patch('transformers.verify_stage.fetch_one',self.query_one),self.assertRaises(ValueError):
            validate_file_stage({**params,'expected':3},chunks)
        with self.engine.begin() as conn:
            conn.execute(text(f'UPDATE {self.schema}.yellow_taxi_canonical SET batch_id=999'))
        with patch('transformers.verify_stage.fetch_one',self.query_one),self.assertRaises(ValueError):
            validate_file_stage(params,chunks)

    def test_retry_counts_mismatch_payload_and_duplicate_input(self):
        raw=self.rows().iloc[[0]].copy();counts={}
        loader.upsert_dataframe(raw,self.schema,'yellow_taxi_raw',metrics=counts)
        loader.upsert_dataframe(raw,self.schema,'yellow_taxi_raw',metrics=counts)
        self.assertEqual(counts,{'attempted':2,'inserted':1,'conflicted':1})
        raw.loc[0,'total_amount']=123
        with self.assertRaisesRegex(ValueError,'Conflicting'):
            loader.upsert_dataframe(raw,self.schema,'yellow_taxi_raw')
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            loader.upsert_dataframe(pd.concat([raw,raw]),self.schema,'yellow_taxi_raw')

    def test_copy_text_null_and_empty_are_distinct(self):
        values=[None,'',r'\N','a,"b"\nnext',r'c:\folder\file']
        frame=pd.DataFrame({'source_record_key':[str(i) for i in range(len(values))],'value':values})
        loader.upsert_dataframe(frame,self.schema,'copy_strings')
        with self.engine.begin() as conn:
            got=conn.execute(text(f'SELECT value FROM {self.schema}.copy_strings ORDER BY source_record_key')).scalars().all()
        self.assertEqual(got,values)

    def test_reject_all_and_extreme_numeric(self):
        raw=self.rows();raw['total_amount']=['invalid','inf']
        raw['passenger_count']=[40000,1.5]
        raw['trip_distance']=[1e99,float('inf')]
        self.load_and_validate(raw)
        accepted,rejected=canonicalize_dataframe(raw)
        self.assertEqual(len(accepted),0)
        self.assertEqual(len(rejected),2)
        self.assertEqual(rejected.iloc[0].raw_payload['total_amount'],'invalid')

    def test_chunk_split_manifest_and_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw=self.rows().drop(columns=['source_record_key','source_row_number','source_file_sha256','batch_id','file_version_id','source_file','source_year_month','schema_fingerprint','schema_version'])
            source=Path(tmp)/'source.parquet';raw.to_parquet(source,index=False)
            file={'resolved_path':str(source),'file_name':'file.parquet','source_year_month':'2024-05','source_file_sha256':'a'*64,'schema_fingerprint':'b'*64,'schema_version':'yt_schema_v001','last_modified':'2024-06-01','parquet_row_count':2,'etl_file_id':1,'file_version_id':101}
            context=load_source_files({'batch_id':10,'files':[file]},work_root=tmp,source_batch_size=2)
            canonical=canonicalize_schema(context,work_root=tmp,canonical_batch_size=1)
            self.assertEqual([c['load_raw_part'] for c in canonical['chunks']],[True,False])
            self.assertEqual([c['source_part_rows'] for c in canonical['chunks']],[2,2])
            self.assertEqual(standardize_and_validate(canonical)['metrics']['rows_raw'],2)
            import data_exporters.load_stage as exporter
            counts=[]
            original=loader.upsert_dataframe
            def routed(frame,schema,table,**kwargs):
                counts.append((table,len(frame)))
                return original(frame,self.schema,table,**kwargs)
            with patch.object(exporter,'reload',lambda module:module),patch.object(exporter,'require_tables'),patch.object(exporter,'fetch_one',return_value=None),patch.object(exporter,'execute'),patch.object(loader,'upsert_dataframe',routed):
                result=exporter.load_stage(canonical)
            self.assertEqual([n for table,n in counts if table=='yellow_taxi_raw'],[2])
            self.assertEqual(result['raw_write_attempts'],2)

    def test_raw_invalid_fails_closed_without_payload_column(self):
        with self.engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE {self.schema}.yellow_taxi_raw DROP COLUMN raw_payload'))
        try:
            with self.assertRaisesRegex(ValueError,'requires reviewed raw_payload migration'):
                loader.upsert_dataframe(self.rows(),self.schema,'yellow_taxi_raw')
        finally:
            with self.engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE {self.schema}.yellow_taxi_raw ADD COLUMN raw_payload jsonb'))

    def test_unknown_and_missing_columns_fail(self):
        raw=self.rows().iloc[[0]].copy()
        with self.assertRaisesRegex(ValueError,'column contract'):
            loader.upsert_dataframe(raw.assign(unmapped_source=1),self.schema,'yellow_taxi_raw')
        with self.assertRaisesRegex(ValueError,'column contract'):
            loader.upsert_dataframe(raw.drop(columns=['file_version_id']),self.schema,'yellow_taxi_raw')

    def test_failed_verify_records_failure_never_success(self):
        from transformers.verify_stage import verify_stage
        context={'batch_id':10,'files':[{'file_name':'missing','file_version_id':101,'source_file_sha256':'a'*64,'parquet_row_count':2,'schema_version':'yt_schema_v001','schema_fingerprint':'b'*64,'source_year_month':'2024-05'}],
                 'chunks':[],'metrics':{'rows_raw':2,'rows_canonical':2,'rows_technical_quarantined':0}}
        with patch('transformers.verify_stage.execute') as execute,self.assertRaises(ValueError):
            verify_stage(context)
        self.assertEqual(execute.call_count,1)
        self.assertIn('STAGING_VALIDATION_FAILED',execute.call_args.args[0])


if __name__ == '__main__': unittest.main()
