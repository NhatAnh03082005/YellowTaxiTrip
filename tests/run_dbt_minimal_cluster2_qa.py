"""Execute actual dbt T03-T05 against an explicitly isolated local PostgreSQL QA.

Requires the dedicated test server/database, never reads .env or production profile.
Creates/truncates only fixture tables in that dedicated database. PostgreSQL 18
local verification does not replace PostgreSQL 16/PostGIS integration in T09.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import subprocess
import sys
import io
import unittest
from unittest.mock import patch
from decimal import Decimal

import psycopg2
from psycopg2.extras import Json, RealDictCursor

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tests'))
from test_dbt_minimal_contract import compare_actual_rows


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--port',type=int,required=True)
    parser.add_argument('--database',required=True)
    parser.add_argument('--user',required=True)
    args=parser.parse_args()
    if (args.port,args.database,args.user)!=(15438,'dbt_minimal_cluster2','dbt_minimal_qa'):
        parser.error('Only dedicated local QA port/database/user is supported; never target live')
    conn=psycopg2.connect(host='127.0.0.1',port=args.port,dbname=args.database,user=args.user)
    conn.autocommit=True
    def query(sql,params=None):
        with conn.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(sql,params)
            return c.fetchall() if c.description else []
    assert query('select current_database() db')[0]['db']==args.database
    fixtures=json.loads((ROOT/'tests/fixtures/dbt_minimal/input.json').read_text())
    expected=json.loads((ROOT/'tests/expected_results/dbt_minimal_rows.json').read_text())
    summaries=json.loads((ROOT/'tests/expected_results/dbt_minimal_summaries.json').read_text())
    out=ROOT/'reports/dbt_minimal_refactor/cluster2';out.mkdir(parents=True,exist_ok=True)
    work=ROOT/'tmp/dbt_minimal_cluster2_qa';work.mkdir(parents=True,exist_ok=True)
    profile=ROOT/'tmp/dbt_minimal_cluster2_profile'
    events=[]
    def dbt(command,version=101,batch=None,extra=None,ok=True,expected_error=None):
        vars={'dbt_minimal_mode':True,'load_batch_id':batch or version,'load_file_version_id':version}
        vars.update(extra or {})
        cmd=[str(ROOT/'.venv-dbt/Scripts/dbt.exe'),*command,'--project-dir',str(ROOT/'dbt_project'),
             '--profiles-dir',str(profile),'--target-path',str(work/'target'),
             '--log-path',str(work/'logs'),'--no-send-anonymous-usage-stats','--vars',json.dumps(vars)]
        r=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',errors='replace')
        label=f'{len(events):02}_{command[0]}_{batch or version}'
        (out/(label+'.log')).write_text(r.stdout+r.stderr,encoding='utf-8')
        events.append({'label':label,'command':command,'vars':vars,'exit_code':r.returncode})
        print(label, 'PASS' if r.returncode==0 else 'EXPECTED_FAIL' if not ok else 'FAIL',flush=True)
        if (r.returncode==0)!=ok:
            raise AssertionError(r.stdout+r.stderr)
        if not ok:
            assert expected_error and expected_error in r.stdout+r.stderr, (label,expected_error,r.stdout+r.stderr)
        return r
    # Re-runs reset only this known dedicated QA database; source data is synthetic.
    query('create extension if not exists pgcrypto')
    for schema in ('dds','stage','dq','control','audit'):
        query(f'create schema if not exists {schema}')
    query('''create table if not exists control.source_file_version(
      file_version_id bigint primary key,source_file text,source_year_month char(7),
      source_file_sha256 char(64),schema_fingerprint char(64),schema_version text,
      source_last_modified timestamptz)''')
    query('''create table if not exists control.etl_batch(batch_id bigint primary key,status text,
      pipeline_name text default 'qa',source_name text default 'yellow')''')
    query('''create table if not exists control.etl_file(batch_id bigint,file_version_id bigint,
      file_status text,source_file text,source_file_sha256 char(64))''')
    query('create table if not exists control.etl_replacement(batch_id bigint,old_version_id bigint,new_version_id bigint)')
    query('''create table if not exists stage.yellow_taxi_raw(source_record_key char(64) unique,
      batch_id bigint,file_version_id bigint,source_file text,source_year_month char(7),
      source_file_sha256 char(64),schema_fingerprint char(64),schema_version text,raw_payload jsonb)''')
    query('alter table stage.yellow_taxi_raw add column if not exists source_row_number bigint')
    query('''create table if not exists stage.yellow_taxi_canonical(
      stage_row_id bigint generated always as identity,source_record_key char(64) unique,
      batch_id bigint,file_version_id bigint,source_file text,source_year_month char(7),
      source_file_sha256 char(64),schema_fingerprint char(64),schema_version text,
      source_last_modified timestamptz,ingested_at timestamptz default now(),cast_error_flags jsonb default '{}',raw_payload jsonb,
      vendor_id smallint,pickup_datetime timestamp,dropoff_datetime timestamp,passenger_count smallint,
      trip_distance numeric(10,3),rate_code_id smallint,store_and_fwd_flag char(1),
      pu_location_id smallint,do_location_id smallint,payment_type_id smallint,
      fare_amount numeric(12,2),extra numeric(12,2),mta_tax numeric(12,2),tip_amount numeric(12,2),
      tolls_amount numeric(12,2),improvement_surcharge numeric(12,2),total_amount numeric(12,2),
      congestion_surcharge numeric(12,2),airport_fee numeric(12,2),cbd_congestion_fee numeric(12,2))''')
    query('''create table if not exists dq.technical_quarantine(
      source_record_key char(64) unique,batch_id bigint,file_version_id bigint,source_file text,
      source_file_sha256 char(64),raw_payload jsonb,total_amount_raw text)''')
    query('''create table if not exists stage.taxi_zone_raw(location_id smallint primary key,
      borough text,zone_name text,service_zone text)''')
    query('create table if not exists stage.taxi_zone_geometry(location_id smallint,geometry text)')
    for zone in fixtures['known_zones']:
        query("insert into stage.taxi_zone_raw values(%s,'Queens','Fixture','Fixture') on conflict(location_id) do update set borough=excluded.borough",(zone,))
    # Deliberately legacy-named relations: new mode must not read/write lifecycle/registry.
    query('create table if not exists dq.fact_taxi_trip_history(untouched text)')
    query('create table if not exists audit.trip_fingerprint_history(untouched text)')
    for rel in ('dq.fact_taxi_trip_history','audit.trip_fingerprint_history'):
        query(f"insert into {rel} select 'legacy sentinel' where not exists(select 1 from {rel})")
    for rel in ('dds.fact_taxi_trip','dq.business_quarantine','dq.duplicate_quarantine',
                'dds.dbt_minimal_affected_grains','dds.dbt_minimal_candidate_summary'):
        if query('select to_regclass(%s) r',(rel,))[0]['r']:
            query(f'truncate {rel} restart identity')
    if query("select to_regclass('dds.fact_taxi_trip') r")[0]['r']:
        query('drop trigger if exists qa_fail_fact on dds.fact_taxi_trip')
    for file in fixtures['files']:
        query('''insert into control.source_file_version values(%s,%s,%s,%s,%s,%s,null)
          on conflict(file_version_id) do nothing''',tuple(file[k] for k in
          ('file_version_id','source_file','source_year_month','sha256','schema_fingerprint','schema_version')))
    aliases={'VendorID':'vendor_id','tpep_pickup_datetime':'pickup_datetime','tpep_dropoff_datetime':'dropoff_datetime',
             'RatecodeID':'rate_code_id','PULocationID':'pu_location_id','DOLocationID':'do_location_id',
             'payment_type':'payment_type_id','Airport_fee':'airport_fee'}
    def load(scope,batch=None):
        file=next(f for f in fixtures['files'] if f['id']==scope)
        version=file['file_version_id'];batch=batch or version
        query("insert into control.etl_batch(batch_id,status) values(%s,'DBT_RUNNING') on conflict(batch_id) do update set status='DBT_RUNNING'",(batch,))
        query('delete from control.etl_file where batch_id=%s',(batch,))
        query("insert into control.etl_file values(%s,%s,'STAGED',%s,%s)",(batch,version,file['source_file'],file['sha256']))
        query('truncate stage.yellow_taxi_raw,stage.yellow_taxi_canonical')
        query('delete from dq.technical_quarantine where batch_id=%s',(batch,))
        for row in file['rows']:
            key=expected[row['case_id']]['source_record_key']
            identity=dict(source_record_key=key,batch_id=batch,file_version_id=version,source_file=file['source_file'],
                          source_year_month=file['source_year_month'],source_file_sha256=file['sha256'],
                          schema_fingerprint=file['schema_fingerprint'],schema_version=file['schema_version'],
                          source_row_number=row['source_row_number'])
            cols=list(identity)+['raw_payload'];vals=[*identity.values(),Json(row['raw'])]
            query('insert into stage.yellow_taxi_raw('+','.join(cols)+') values('+','.join(['%s']*len(cols))+')',vals)
            if expected[row['case_id']]['disposition']=='TECHNICAL_REJECT':
                query('insert into dq.technical_quarantine values(%s,%s,%s,%s,%s,%s,%s)',
                      (key,batch,version,file['source_file'],file['sha256'],Json(row['raw']),row['raw']['total_amount']))
            else:
                canonical={aliases.get(k,k):v for k,v in row['raw'].items()}
                identity['source_row_number']=row['source_row_number']
                data=dict(identity,**canonical)
                query('insert into stage.yellow_taxi_canonical('+','.join(data)+') values('+','.join(['%s']*len(data))+')',list(data.values()))
        return file
    # source_row_number is a fixture loader column, as in Mage canonical DDL.
    query('alter table stage.yellow_taxi_canonical add column if not exists source_row_number bigint')
    def verify(scope):
        file=next(f for f in fixtures['files'] if f['id']==scope)
        rows=query('select * from dds.int_taxi_trip_classified')
        mapped={expected[r['case_id']]['source_record_key']:r['case_id'] for r in file['rows']}
        actual=[]
        for r in rows:
            actual.append({'case_id':mapped[r['source_record_key']], 'source_record_key':r['source_record_key'],
              'disposition':r['primary_disposition'],'rules':sorted(r['dq_rule_ids']),
              'has_in_file_candidate_match':r['has_in_file_candidate_match'],
              'eligible':{k:r['is_'+('revenue_kpi_eligible' if k=='revenue' else k+'_kpi_eligible')] for k in ('revenue','tip','speed','duration','passenger')}})
        for r in file['rows']:
            if expected[r['case_id']]['disposition']=='TECHNICAL_REJECT':
                actual.append(dict(expected[r['case_id']],case_id=r['case_id']))
        compare_actual_rows(actual,{r['case_id']:expected[r['case_id']] for r in file['rows']})
        s=summaries[scope];version=file['file_version_id']
        totals=query('select count(*) n,coalesce(sum(revenue_amount),0) revenue from dds.fact_taxi_trip where file_version_id=%s',(version,))[0]
        assert totals['n']==s['fact_rows'] and totals['revenue']==Decimal(s['kpi']['revenue']),(scope,totals)
        for model,key in [('business_quarantine','business_rows'),('duplicate_quarantine','duplicate_rows')]:
            assert query(f'select count(*) n from dq.{model} where file_version_id=%s',(version,))[0]['n']==s[key]
        for field,source_column in [('fare','fare_amount'),('tip','tip_amount'),('total','total_amount')]:
            for branch,table in [('TRUSTED','dds.fact_taxi_trip'),('BUSINESS_QUARANTINE','dq.business_quarantine'),('DUPLICATE','dq.duplicate_quarantine'),('ALL','stage.yellow_taxi_raw'),('TECHNICAL_REJECT','dq.technical_quarantine')]:
                expression=(f"case when pg_input_is_valid(raw_payload->>'{source_column}','numeric(12,2)') then (raw_payload->>'{source_column}')::numeric(12,2) end" if branch in ('ALL','TECHNICAL_REJECT') else source_column)
                actual=query(f'select count({expression}) n,coalesce(sum({expression}),0) amount from {table} where file_version_id=%s',(version,))[0]
                wanted=s['measures'][field][branch]
                assert actual['n']==wanted['eligible_count'] and actual['amount']==Decimal(wanted['sum']),(scope,field,branch,actual,wanted)
        stats=query('''select coalesce(sum(tip_amount) filter(where is_tip_kpi_eligible),0) tip_numerator,
          coalesce(sum(fare_amount) filter(where is_tip_kpi_eligible),0) tip_denominator,
          coalesce(sum(trip_distance_miles) filter(where is_speed_kpi_eligible),0) speed_distance,
          coalesce(sum(duration_seconds) filter(where is_speed_kpi_eligible),0) speed_seconds,
          coalesce(sum(duration_seconds) filter(where is_duration_kpi_eligible),0) duration_sum,
          count(*) filter(where is_duration_kpi_eligible) duration_count,
          coalesce(sum(passenger_count) filter(where is_passenger_kpi_eligible),0) passenger_sum,
          count(*) filter(where is_passenger_kpi_eligible) passenger_count
          from dds.fact_taxi_trip where file_version_id=%s''',(version,))[0]
        for name,value in stats.items(): assert value==Decimal(str(s['kpi'][name])),(scope,name,value,s['kpi'][name])
        for key,dimension,target_key in [('pickup_date_key','dim_date','date_key'),('dropoff_date_key','dim_date','date_key'),
            ('pickup_time_key','dim_time','time_key'),('dropoff_time_key','dim_time','time_key'),
            ('pickup_zone_key','dim_zone','zone_key'),('dropoff_zone_key','dim_zone','zone_key'),
            ('vendor_key','dim_vendor','vendor_key'),('payment_type_key','dim_payment_type','payment_type_key'),('rate_code_key','dim_rate_code','rate_code_key')]:
            assert query(f'select count(*) n from dds.fact_taxi_trip f where file_version_id=%s and (f.{key} is null or not exists(select 1 from dds.{dimension} d where d.{target_key}=f.{key}))',(version,))[0]['n']==0
        return totals
    load('A_2024')
    dbt(['parse'])
    dbt(['seed','--exclude','business_fixture_expectations'])
    dbt(['run','--select','+tag:minimal_dds'])
    verify('A_2024')
    assert query('select zone_name from dds.dim_zone where location_id=132')[0]['zone_name']=='Fixture'
    keys=query('select source_record_key,trip_key,xmin::text version from dds.fact_taxi_trip order by source_record_key')
    references=query('select * from dds.dbt_minimal_reference_state order by model_name')
    dbt(['run','--select','+tag:minimal_dds'])
    verify('A_2024')
    assert keys==query('select source_record_key,trip_key,xmin::text version from dds.fact_taxi_trip order by source_record_key')
    assert references==query('select * from dds.dbt_minimal_reference_state order by model_name')
    # Changed reference refresh without key churn; same content next run skips.
    query("update stage.taxi_zone_raw set borough='Updated QA' where location_id=132")
    dbt(['run','--select','+dim_zone'])
    assert query('select borough from dds.dim_zone where location_id=132')[0]['borough']=='Updated QA'
    assert query('select * from dds.dbt_minimal_reference_state order by model_name')!=references
    vendor_key=query('select vendor_key from dds.dim_vendor where vendor_id=1')[0]['vendor_key']
    query('update dds.vendor_codes set vendor_key=901 where vendor_id=1')
    dbt(['run','--select','dim_vendor'],101,ok=False,expected_error='Reference surrogate mapping drift')
    assert query('select vendor_key from dds.dim_vendor where vendor_id=1')[0]['vendor_key']==vendor_key
    query('update dds.vendor_codes set vendor_key=%s where vendor_id=1',(vendor_key,))
    dbt(['run','--select','dim_date'],101,extra={'minimal_calendar_start':'2024-01-01'},ok=False,expected_error='Refusing to shrink existing calendar')
    for scope,version in [('B_2025',102),('C_CROSS_FILE',103)]:
        load(scope);dbt(['run','--select','+tag:minimal_dds'],version);verify(scope)
    assert query('select count(*) n from dds.fact_taxi_trip')[0]['n']==23
    assert query('select sum(revenue_amount) n from dds.fact_taxi_trip')[0]['n']==Decimal('320.03')
    unrelated=query('select source_record_key,trip_key from dds.fact_taxi_trip where file_version_id in (102,103) order by source_record_key')
    query("update control.etl_batch set status='SUCCESS' where batch_id=101")
    load('A_REPLACEMENT')
    dbt(['run','--select','+int_taxi_trip_classified'],104,ok=False,expected_error='Changed source version requires explicit approved replacement')
    query('delete from control.etl_replacement where batch_id=104')
    query('insert into control.etl_replacement values(104,101,104)')
    before_replace=query('select source_record_key,trip_key from dds.fact_taxi_trip order by source_record_key')
    query("""create or replace function dds.qa_fail_fact() returns trigger language plpgsql as $$
      begin raise exception 'QA forced insert failure after scoped delete'; end $$""")
    query('create trigger qa_fail_fact before insert on dds.fact_taxi_trip for each row execute function dds.qa_fail_fact()')
    dbt(['run','--select','+tag:minimal_dds'],104,extra={'apply_approved_fact_replacement':True},ok=False,expected_error='QA forced insert failure after scoped delete')
    assert before_replace==query('select source_record_key,trip_key from dds.fact_taxi_trip order by source_record_key')
    query('drop trigger qa_fail_fact on dds.fact_taxi_trip')
    dbt(['run','--select','+tag:minimal_dds'],104,extra={'apply_approved_fact_replacement':True})
    verify('A_REPLACEMENT')
    assert query('select count(*) n from dds.fact_taxi_trip')[0]['n']==7
    assert query('select sum(revenue_amount) n from dds.fact_taxi_trip')[0]['n']==Decimal('111.00')
    assert unrelated==query('select source_record_key,trip_key from dds.fact_taxi_trip where file_version_id in (102,103) order by source_record_key')
    assert query('select count(*) n from dq.business_quarantine where file_version_id=101')[0]['n']==0
    assert query('select count(*) n from dq.duplicate_quarantine where file_version_id=101')[0]['n']==0
    assert {r['pickup_date_key'] for r in query('select pickup_date_key from dds.dbt_minimal_affected_grains where batch_id=104')}=={20240501,20240502}
    # Existing registry/history rows must not be required or mutated.
    for rel in ('dq.fact_taxi_trip_history','audit.trip_fingerprint_history'):
        assert query(f'select * from {rel}')==[{'untouched':'legacy sentinel'}]
    # Schema and calendar failure checks run before persistent Fact mutation.
    load('A_2024')
    dbt(['run','--select','fact_taxi_trip'],101,ok=False,expected_error='Classification workspace belongs to another batch/version')
    # Actual out-of-range test: fixture source date beyond the physically initialized calendar.
    query("update stage.yellow_taxi_canonical set pickup_datetime='2030-01-01',dropoff_datetime='2030-01-01 00:10'")
    dbt(['run','--select','+int_taxi_trip_accepted'],101)  # rebuild dependent view after workspace table swap
    before=query('select source_record_key,trip_key from dds.fact_taxi_trip order by source_record_key')
    dbt(['run','--select','fact_taxi_trip'],101,ok=False,expected_error='Valid source date outside calendar')
    assert before==query('select source_record_key,trip_key from dds.fact_taxi_trip order by source_record_key')
    # Missing raw field is blocked by workspace CHECK; previous workspace transaction remains intact.
    load('A_2024');query("update stage.yellow_taxi_raw set raw_payload=raw_payload-'extra' where source_record_key=%s",(expected['A01_CONTROL']['source_record_key'],))
    dbt(['run','--select','+int_taxi_trip_classified'],101,ok=False,expected_error='minimal_raw_input_valid')
    assert query("select valid from dds.dbt_minimal_workspace_state where model_name='int_taxi_trip_classified'")[0]['valid'] is False
    dbt(['run','--select','fact_taxi_trip'],101,ok=False,expected_error='Classification workspace belongs to another batch/version')
    load('A_2024')
    query("delete from dds.dq_rule_policy where rule_id='AMT-001'")
    dbt(['run','--select','+int_taxi_trip_classified'],101,ok=False,expected_error='minimal_raw_input_valid')
    query("insert into dds.dq_rule_policy values('AMT-001','HIGH','BUSINESS_QUARANTINE',3)")
    load('A_2024');query('update stage.yellow_taxi_raw set batch_id=999 where source_row_number=1')
    dbt(['run','--select','+int_taxi_trip_classified'],101,ok=False,expected_error='Mixed or inconsistent minimal staging scope')
    # Legacy flags must fail loudly.
    dbt(['run','--select','+int_taxi_trip_classified'],101,extra={'fact_lifecycle_mode':'archive'},ok=False,expected_error='Minimal mode does not support archive')
    load('ALL_REJECTED');dbt(['run','--select','+tag:minimal_dds'],106);verify('ALL_REJECTED')
    # Put a stale row into this QA version; zero incoming accepted/DQ must clear it.
    query("update dds.fact_taxi_trip set file_version_id=106 where source_record_key=%s",(expected['C01_CROSS_FILE_SAME_RAW']['source_record_key'],))
    for table in ('business_quarantine','duplicate_quarantine'):
        columns=[r['column_name'] for r in query("select column_name from information_schema.columns where table_schema='dq' and table_name=%s order by ordinal_position",(table,))]
        selections=["repeat('0',64)" if c=='source_record_key' else '106' if c in ('file_version_id','batch_id') else '2' if c=='duplicate_occurrence_number' else 'source_record_key' if c=='duplicate_survivor_source_record_key' else c for c in columns]
        query(f"insert into dq.{table}({','.join(columns)}) select {','.join(selections)} from dq.business_quarantine where file_version_id=102 limit 1")
    dbt(['run','--select','fact_taxi_trip','business_quarantine','duplicate_quarantine'],106)
    assert query('select count(*) n from dds.fact_taxi_trip where file_version_id=106')[0]['n']==0
    for table in ('business_quarantine','duplicate_quarantine'):
        assert query(f'select count(*) n from dq.{table} where file_version_id=106')[0]['n']==0
    load('EMPTY_VALIDATED_SCOPE');dbt(['run','--select','+tag:minimal_dds'],105);verify('EMPTY_VALIDATED_SCOPE')
    # A target with legacy extra columns must fail explicitly, never auto-migrate/drop.
    query('alter table dds.fact_taxi_trip add column legacy_extra text')
    dbt(['run','--select','fact_taxi_trip'],105,ok=False,expected_error='Minimal schema differs')
    assert query("select count(*) n from information_schema.columns where table_schema='dds' and table_name='fact_taxi_trip' and column_name='legacy_extra'")[0]['n']==1
    query('alter table dds.fact_taxi_trip drop column legacy_extra')  # only this synthetic QA alteration
    # Execute existing legacy SQL vectors on this QA via temp tables/ROLLBACK.
    # Route ONLY the known docker-psql invocation, never run a container/live command.
    original_run=subprocess.run
    def qa_psql(command,*positional,**kwargs):
        if command[:4]!=['docker','compose','exec','-T']:
            raise AssertionError('Unexpected subprocess in legacy SQL fixture')
        command=[r'C:\Program Files\PostgreSQL\18\bin\psql.exe','-X','-v','ON_ERROR_STOP=1',
                 '-h','127.0.0.1','-p',str(args.port),'-U',args.user,'-d',args.database]
        return original_run(command,*positional,**kwargs)
    suite=unittest.TestSuite()
    for module in ('test_fingerprint_golden_versions','test_duplicate_survivor_fixture_sql',
                   'test_olap_kpi_fixture_sql','test_fact_lifecycle_scope','test_fingerprint_consumer_switch'):
        suite.addTests(unittest.defaultTestLoader.loadTestsFromName(module))
    stream=io.StringIO()
    with patch('subprocess.run',side_effect=qa_psql):
        legacy_result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
    (out/'legacy_sql_and_unit_tests.log').write_text(stream.getvalue(),encoding='utf-8')
    assert legacy_result.wasSuccessful(),stream.getvalue()
    dbt(['parse'],105,extra={'dbt_minimal_mode':False})
    result={'status':'PASS','postgres':query('select version() version')[0]['version'],
      'fixture_scope_totals':summaries,'dbt_invocations':events,
      'legacy_selected_tests':legacy_result.testsRun,
      'limitations':['PostgreSQL 18 local QA, not production 16/PostGIS','Technical fixture routing supplied by literal spec; Mage not executed','Runner/OLAP/final tests selectors belong to T06-T09']}
    (out/'results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
    conn.close()
    print('Actual dbt cluster2 QA PASS',flush=True)


if __name__=='__main__':
    main()
