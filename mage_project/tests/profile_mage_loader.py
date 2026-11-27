"""Opt-in QA profile of already-produced bounded parts. Never invokes control writes."""
import argparse
import json
import time
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from sqlalchemy import text
from utils.stage_loader import postgres_engine, upsert_dataframe
from utils.profiling import phase
from transformers.verify_stage import validate_file_stage


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--qa', action='store_true', required=True)
    parser.add_argument('--parts', required=True, type=Path)
    parser.add_argument('--limit', type=int, default=0)
    args = parser.parse_args()
    schema = 'mage_m00_profile_' + uuid.uuid4().hex[:12]
    engine = postgres_engine()
    start = time.perf_counter()
    def stats(conn):
        return dict(conn.execute(text("SELECT temp_bytes, pg_database_size(current_database()) db_bytes, pg_current_wal_lsn()::text wal_lsn FROM pg_stat_database WHERE datname=current_database() ")).mappings().one())
    def reader(query, params):
        import re
        query = query.replace('stage.yellow_',schema+'.yellow_').replace('dq.technical_quarantine',schema+'.technical_quarantine')
        with engine.begin() as conn:
            return dict(conn.execute(text(re.sub(r'%\((\w+)\)s',r':\1',query)),params).mappings().one())
    try:
        with engine.begin() as conn:
            before=stats(conn)
            conn.execute(text('CREATE SCHEMA '+schema))
            for name, src in [('yellow_taxi_raw','stage'),('yellow_taxi_canonical','stage'),('technical_quarantine','dq')]:
                conn.execute(text(f'CREATE UNLOGGED TABLE {schema}.{name} (LIKE {src}.{name} INCLUDING ALL)'))
        raws = sorted((args.parts/'raw').glob('*.parquet'))
        canonicals=sorted((args.parts/'canonical').glob('*.parquet'))
        if len(raws)!=len(canonicals) or not raws:
            raise ValueError('Baseline profile requires original one-to-one source/canonical parts')
        if args.limit: raws=raws[:args.limit];canonicals=canonicals[:args.limit]
        chunks=[];counts={};identity=None
        for raw_path,canonical_path in zip(raws,canonicals):
            with phase('read_raw_parquet'):
                raw=pd.read_parquet(raw_path)
            with phase('read_canonical_parquet'):
                canonical=pd.read_parquet(canonical_path)
            if len(raw)!=len(canonical):
                raise ValueError('Profile supports zero-reject baseline only')
            identity=raw.iloc[0]
            for frame,name in [(raw,'yellow_taxi_raw'),(canonical,'yellow_taxi_canonical')]:
                upsert_dataframe(frame,schema,name,metrics=counts.setdefault(name,{}))
            chunks.append({'source_file':str(identity.source_file),'row_count':len(raw),'canonical_rows':len(canonical),'technical_reject_rows':0})
        params={'batch':int(identity.batch_id),'version':int(identity.file_version_id),'sha':str(identity.source_file_sha256),'file':str(identity.source_file),'expected':sum(c['row_count'] for c in chunks),'schema':str(identity.schema_version),'fingerprint':str(identity.schema_fingerprint),'month':str(identity.source_year_month)}
        with phase('validation'),patch('transformers.verify_stage.fetch_one',reader):
            validate_file_stage(params,chunks)
        with engine.begin() as conn:
            sizes=[dict(row) for row in conn.execute(text("SELECT relname,pg_relation_size(relid) heap_bytes,pg_indexes_size(relid) index_bytes,pg_total_relation_size(relid) total_bytes FROM pg_stat_user_tables WHERE schemaname=:schema"),{'schema':schema}).mappings()]
            after=stats(conn)
            wal_delta=conn.execute(text('SELECT pg_wal_lsn_diff(:after,:before)'),{'after':after['wal_lsn'],'before':before['wal_lsn']}).scalar()
        print('MAGE_QA_RESULT '+json.dumps({'scope':'loader + validation; existing local parts; no Mage scheduler/source transformation','rows':params['expected'],'source_sha256':params['sha'],'counts':counts,'relations':sizes,'seconds':time.perf_counter()-start,'before':before,'after':after,'cluster_wal_delta_bytes':int(wal_delta),'note':'Global WAL/temp stats may include other activity; cloned tables omit foreign keys, retain indexes/checks/identities.'}),flush=True)
    finally:
        assert schema.startswith('mage_m00_profile_') and len(schema)==29
        with engine.begin() as conn:
            conn.execute(text('DROP SCHEMA IF EXISTS '+schema+' CASCADE'))
        engine.dispose()


if __name__ == '__main__': main()
