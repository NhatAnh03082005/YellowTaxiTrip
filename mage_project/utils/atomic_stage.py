"""Opt-in atomic Direct COPY. Never deletes existing staging or advances watermark."""
import hashlib
import json

import pandas as pd
from sqlalchemy import text

from .database import execute
from .profiling import phase
from .stage_loader import StageWriter, _safe_identifier


def signature(context):
    files = [{k: f[k] for k in ('etl_file_id','file_version_id','source_file_sha256',
                               'file_name','schema_version','schema_fingerprint',
                               'source_year_month','parquet_row_count')} for f in context['files']]
    descriptor=({'source_rows':sum(int(f['parquet_row_count']) for f in context['files']),'prepare_mode':'streaming'}
                if context.get('prepare_mode')=='streaming' else context['metrics'])
    payload={'batch':int(context['batch_id']),'files':files,'metrics':descriptor}
    if context.get('stage_storage_mode')=='slim': payload['storage']='slim'
    return hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()



def params_for(context):
    if len(context['files']) != 1:
        raise ValueError('Direct mode requires exactly one file/version; use explicit upsert for other scopes')
    f = context['files'][0]
    if int(f['parquet_row_count']) <= 0:
        raise ValueError('Direct COPY requires a nonempty validated file')
    return {'batch':int(context['batch_id']), 'version':int(f['file_version_id']),
            'sha':f['source_file_sha256'], 'file':f['file_name'],
            'expected':int(f['parquet_row_count']), 'schema':f['schema_version'],
            'fingerprint':f['schema_fingerprint'], 'month':f['source_year_month']}


def state(connection, batch, control):
    return connection.execute(text(f"""
      SELECT b.status batch_status,b.error_code,s.status staging_status,s.details
      FROM {control}.etl_batch b JOIN {control}.etl_step_run s USING(batch_id)
      WHERE b.batch_id=:batch AND s.step_name='STAGING'
    """),{'batch':batch}).mappings().first()


def reader_for(connection, stage, dq):
    def read(query, params):
        query = query.replace('stage.yellow_',stage+'.yellow_').replace('dq.technical_quarantine',dq+'.technical_quarantine')
        return dict(connection.exec_driver_sql(query,params).mappings().one())
    return read


def slim_load_indexes(connection,stage,create,contract='slim-v1'):
    """Drop/rebuild only redundant per-row enforcement indexes while target is empty.

    DDL is in the same transaction as COPY. A failure therefore restores the
    empty constrained tables; publication happens only after all four indexes
    have been rebuilt successfully.
    """
    stage=_safe_identifier(stage)
    if contract == 'slim-v2':
        definitions=(
            ('yellow_taxi_trip_data','yellow_taxi_trip_data_pkey','PRIMARY KEY (trip_row_id)'),
            ('yellow_taxi_trip_data','yellow_taxi_trip_data_source_record_key_key','UNIQUE (source_record_key)'),
        )
    elif contract == 'slim-v1':
        definitions=(
            ('yellow_taxi_raw_data','yellow_taxi_raw_data_pkey','PRIMARY KEY (raw_row_id)'),
            ('yellow_taxi_raw_data','yellow_taxi_raw_data_source_record_key_key','UNIQUE (source_record_key)'),
            ('yellow_taxi_canonical_data','yellow_taxi_canonical_data_pkey','PRIMARY KEY (stage_row_id)'),
            ('yellow_taxi_canonical_data','yellow_taxi_canonical_data_source_record_key_key','UNIQUE (source_record_key)'),
        )
    else:
        raise ValueError('Unsupported slim contract: '+str(contract))
    if create:
        for table,name,definition in definitions:
            connection.execute(text(f'ALTER TABLE {stage}.{table} ADD CONSTRAINT {name} {definition}'))
    else:
        for table,name,_ in definitions:
            connection.execute(text(f'ALTER TABLE {stage}.{table} DROP CONSTRAINT {name}'))


def verify_published(context, *, engine=None, stage='stage', dq='dq', control='control'):
    """Reuse the committed key validation, checking DB receipt and crash-sensitive counts.

    Only direct transactions create this receipt. No writer may change a STAGED
    attempt. UNLOGGED truncation/partial loss invalidates counts, requiring recovery.
    """
    stage,dq,control = map(_safe_identifier,(stage,dq,control))
    params=params_for(context)
    with StageWriter(engine=engine) as writer:
        connection=writer.connection
        row=state(connection,params['batch'],control)
        if (not row or row['batch_status']!='STAGED' or row['staging_status']!='SUCCESS'
                or row['details'].get('loader_mode')!='direct'
                or row['details'].get('manifest_signature')!=signature(context)):
            raise ValueError('Missing or mismatched atomic staging receipt; do not reload/force SUCCESS')
        counts=connection.execute(text(f"""
          SELECT (SELECT count(*) FROM {stage}.yellow_taxi_raw) raw,
                 (SELECT count(*) FROM {stage}.yellow_taxi_canonical) canonical,
                 (SELECT count(*) FROM {dq}.technical_quarantine
                   WHERE batch_id=:batch AND file_version_id=:version) rejected
        """),params).mappings().one()
        expected=context['metrics']
        if context.get('prepare_mode')=='streaming' and context.get('metrics_pending') and expected['rows_canonical']<0:
            expected={k:row['details'][k] for k in ('rows_raw','rows_canonical','rows_technical_quarantined')}
            if expected['rows_raw']!=params['expected']:
                raise ValueError('Streaming receipt footer mismatch')
        if (counts['raw'],counts['canonical'],counts['rejected']) != (
                expected['rows_raw'],expected['rows_canonical'],expected['rows_technical_quarantined']):
            raise ValueError('Published staging data is missing; possible UNLOGGED loss. Recovery required')
        return {'batch_id':params['batch'],'status':'STAGED','rows_raw':counts['raw'],
                'rows_canonical':counts['canonical'],'rows_technical_quarantined':counts['rejected'],
                'local_metrics':expected,'stage_load_mode':'direct'}


def load_direct(context, *, engine=None, stage='stage', dq='dq', control='control',
                heartbeat=execute, fault=None):
    """One file, all three tables and STAGED control changes in one transaction.

    QA can supply independent schemas/engine and inject faults. These parameters
    are not runtime variables exposed by the Mage exporter.
    """
    from transformers.verify_stage import validate_file_stage,validate_unified_file_stage
    stage,dq,control=map(_safe_identifier,(stage,dq,control))
    params=params_for(context)
    batch=params['batch']
    receipt=signature(context)
    started=False
    committed=False
    locked=False
    counts={'raw':{},'canonical':{},'reject':{}}
    def checkpoint(name):
        if fault is not None: fault(name)
    lock_name='yellow_taxi_staging_writer' if stage=='stage' else 'yellow_taxi_staging_writer:'+stage
    with StageWriter(engine=engine,storage_mode=context.get('stage_storage_mode','wide'),control_schema=control,batch_id=batch) as writer:
        connection=writer.connection
        try:
            locked=connection.execute(text("SELECT pg_try_advisory_lock(hashtext(:name))"),{'name':lock_name}).scalar()
            if not locked: raise RuntimeError('Another staging writer holds the lock')
            row=state(connection,batch,control)
            if row and row['batch_status']=='STAGED':
                result=verify_published(context,engine=writer.engine,stage=stage,dq=dq,control=control)
                return {**context,'stage_load_mode':'direct','direct_stage_published':True,
                        'direct_reused':True,'write_counts':{},'published_counts':result}
            if not row or row['batch_status'] not in ('RUNNING','FAILED'):
                raise ValueError('Batch is not writable; SUCCESS must be skipped upstream')
            if row['staging_status'] not in ('PENDING','FAILED'):
                raise ValueError('STAGING is not pending/failed; recovery must resolve the previous worker first')
            # File validation must be backed by control, not just a client context.
            f=context['files'][0]
            file_row=connection.execute(text(f"""
              SELECT 1 FROM {control}.etl_file f
              JOIN {control}.source_file_version v USING(file_version_id)
              WHERE f.etl_file_id=:id AND f.batch_id=:batch
                AND f.file_version_id=:version AND f.source_file=:file AND f.source_file_sha256=:sha
                AND f.schema_version=:schema AND f.schema_fingerprint=:fingerprint AND f.source_year_month=:month
                AND v.source_file=f.source_file AND v.source_file_sha256=f.source_file_sha256
                AND v.schema_version=f.schema_version AND v.schema_fingerprint=f.schema_fingerprint
                AND v.source_year_month=f.source_year_month
                AND f.file_status IN ('VALIDATED','APPROVED_SOURCE_VERSION')
            """),{**params,'id':int(f['etl_file_id'])}).first()
            if not file_row: raise ValueError('Direct COPY requires matching validated control file')
            with connection.begin():
                # Session-local only: preserve durability while giving the single
                # deferred index build enough memory and I/O concurrency.
                connection.execute(text("SET LOCAL work_mem='128MB'"))
                connection.execute(text("SET LOCAL maintenance_work_mem='512MB'"))
                connection.execute(text("SET LOCAL effective_io_concurrency=200"))
                connection.execute(text("SET LOCAL max_parallel_maintenance_workers=2"))
                slim_contract=None
                if context.get('stage_storage_mode')=='slim':
                    slim_contract=connection.execute(text(
                        f'SELECT storage_mode FROM {stage}.stage_storage_contract WHERE singleton'
                    )).scalar()
                    if slim_contract not in ('slim-v1','slim-v2'):
                        raise ValueError('Slim storage contract is missing or unsupported')
                if slim_contract=='slim-v2':
                    if context.get('prepare_mode')!='streaming':
                        raise ValueError('Slim-v2 requires bounded streaming preparation')
                    raw_target=canonical_target='yellow_taxi_trip_data'
                else:
                    raw_target=writer.target(stage,'yellow_taxi_raw')
                    canonical_target=writer.target(stage,'yellow_taxi_canonical')
                extra_lock=f',{stage}.stage_file_batch' if context.get('stage_storage_mode')=='slim' else ''
                lock_mode='ACCESS EXCLUSIVE' if context.get('stage_storage_mode')=='slim' else 'SHARE ROW EXCLUSIVE'
                targets=f'{stage}.{raw_target}'
                if canonical_target!=raw_target: targets+=f',{stage}.{canonical_target}'
                connection.execute(text(f'LOCK TABLE {targets},{dq}.technical_quarantine{extra_lock} IN {lock_mode} MODE NOWAIT'))
                occupied=connection.execute(text(f"""
                  SELECT EXISTS(SELECT 1 FROM {stage}.yellow_taxi_raw)
                    OR EXISTS(SELECT 1 FROM {stage}.yellow_taxi_canonical)
                    OR EXISTS(SELECT 1 FROM {dq}.technical_quarantine
                              WHERE batch_id=:batch OR file_version_id=:version OR source_file_sha256=:sha)
                """),params).scalar()
                if context.get('stage_storage_mode')=='slim':
                    occupied=occupied or connection.execute(text(f'SELECT EXISTS(SELECT 1 FROM {stage}.stage_file_batch)')).scalar()
                if occupied:
                    raise ValueError('Direct target is not clean; no automatic delete/fallback. Finish pending DDS/cleanup or reviewed recovery')
                if context.get('stage_storage_mode')=='slim':
                    with phase('slim_index_drop'):
                        slim_load_indexes(connection,stage,False,slim_contract)
                    checkpoint('after_slim_index_drop')
                if row['batch_status']=='FAILED':
                    heartbeat(f'SELECT {control}.resume_failed_staging(%(batch)s);',{'batch':batch})
                heartbeat(f"SELECT {control}.start_step(%(batch)s,'STAGING');",{'batch':batch})
                started=True
                if context.get('stage_storage_mode')=='slim':
                    connection.execute(text(f'INSERT INTO {stage}.stage_file_batch(file_version_id,batch_id,source_last_modified) SELECT file_version_id,batch_id,source_last_modified FROM {control}.etl_file WHERE etl_file_id=:id AND batch_id=:batch AND file_version_id=:version'),{**params,'id':int(f['etl_file_id'])})
                generated=[]
                totals={'rows_raw':0,'rows_canonical':0,'rows_technical_quarantined':0}
                if slim_contract=='slim-v2':
                    from .stream_parts import iter_unified_parts
                    def on_part(index,part_counts):
                        generated.append(part_counts)
                        totals['rows_raw']+=part_counts['row_count']
                        totals['rows_canonical']+=part_counts['canonical_rows']
                        totals['rows_technical_quarantined']+=part_counts['technical_reject_rows']
                        if index%10==0:
                            heartbeat(f'SELECT {control}.touch_batch_heartbeat(%(batch)s);',{'batch':batch})
                    writer.write_stream(
                        iter_unified_parts(context),stage,'yellow_taxi_trip_data',
                        metrics=counts['raw'],on_part=on_part,
                    )
                    checkpoint('after_unified')
                    with phase('derive_technical_quarantine'):
                        rejected=connection.execute(text(f"""
                          INSERT INTO {dq}.technical_quarantine(
                            vendor_id,pickup_datetime,dropoff_datetime,passenger_count,
                            trip_distance,rate_code_id,store_and_fwd_flag,pu_location_id,
                            do_location_id,payment_type_id,fare_amount,extra,mta_tax,
                            tip_amount,tolls_amount,improvement_surcharge,total_amount,
                            congestion_surcharge,airport_fee,cbd_congestion_fee,
                            cast_error_flags,raw_payload,batch_id,file_version_id,
                            source_file,source_year_month,source_file_sha256,
                            source_row_number,source_record_key,schema_fingerprint,
                            schema_version,source_last_modified)
                          SELECT d.vendor_id,d.pickup_datetime,d.dropoff_datetime,d.passenger_count,
                            d.trip_distance,d.rate_code_id,d.store_and_fwd_flag,d.pu_location_id,
                            d.do_location_id,d.payment_type_id,d.fare_amount,d.extra,d.mta_tax,
                            d.tip_amount,d.tolls_amount,d.improvement_surcharge,d.total_amount,
                            d.congestion_surcharge,d.airport_fee,d.cbd_congestion_fee,
                            d.cast_error_flags,d.raw_payload,m.batch_id,d.file_version_id,
                            v.source_file,v.source_year_month,v.source_file_sha256,
                            d.source_row_number,d.source_record_key,v.schema_fingerprint,
                            v.schema_version,m.source_last_modified
                          FROM {stage}.yellow_taxi_trip_data d
                          JOIN {stage}.stage_file_batch m USING(file_version_id)
                          JOIN {control}.source_file_version v USING(file_version_id)
                          WHERE NOT d.technical_valid
                        """ )).rowcount
                    counts['canonical']={'attempted':totals['rows_canonical'],'inserted':totals['rows_canonical'],'conflicted':0}
                    counts['reject']={'attempted':rejected,'inserted':rejected,'conflicted':0}
                    if rejected!=totals['rows_technical_quarantined']:
                        raise ValueError('Slim-v2 quarantine derivation count mismatch')
                    checkpoint('after_reject')
                else:
                    if context.get('prepare_mode')=='streaming':
                        from .stream_parts import iter_parts
                        parts=iter_parts(context)
                    else:
                        parts=iter_disk_parts(context)
                    for index,(raw,canonical,rejected,part_counts) in enumerate(parts,start=1):
                        if raw is not None:
                            writer.write(raw,stage,'yellow_taxi_raw',metrics=counts['raw'],direct=True)
                            checkpoint('after_raw')
                        writer.write(canonical,stage,'yellow_taxi_canonical',metrics=counts['canonical'],direct=True)
                        checkpoint('after_canonical')
                        writer.write(rejected,dq,'technical_quarantine',metrics=counts['reject'],direct=True)
                        checkpoint('after_reject')
                        generated.append(part_counts)
                        totals['rows_raw']+=part_counts['row_count']
                        totals['rows_canonical']+=part_counts['canonical_rows']
                        totals['rows_technical_quarantined']+=part_counts['technical_reject_rows']
                        if index%10==0:
                            heartbeat(f'SELECT {control}.touch_batch_heartbeat(%(batch)s);',{'batch':batch})
                if context.get('prepare_mode')=='streaming':
                    context={**context,'chunks':generated,'metrics':totals,'metrics_pending':False}
                with phase('analyze_stage'):
                    connection.execute(text(f'ANALYZE {stage}.{raw_target};'))
                    if canonical_target!=raw_target:
                        connection.execute(text(f'ANALYZE {stage}.{canonical_target};'))
                    if context.get('stage_storage_mode')=='slim':
                        connection.execute(text(f'ANALYZE {stage}.stage_file_batch;'))
                checkpoint('before_verify')
                with phase('validation',mode='direct'):
                    if slim_contract=='slim-v2':
                        actual=validate_unified_file_stage(
                            params,context['chunks'],
                            reader=lambda query,values: dict(
                                connection.exec_driver_sql(query,values).mappings().one()
                            ),
                            stage=stage,dq=dq,control=control,
                        )
                    else:
                        actual=validate_file_stage(params,context['chunks'],reader=reader_for(connection,stage,dq))
                    if (actual['raw_count'],actual['canonical_count'],actual['rejected_count']) != (
                            context['metrics']['rows_raw'],context['metrics']['rows_canonical'],context['metrics']['rows_technical_quarantined']):
                        raise ValueError('Direct metrics do not match DB counts')
                if context.get('stage_storage_mode')=='slim':
                    with phase('slim_index_build'):
                        slim_load_indexes(connection,stage,True,slim_contract)
                    checkpoint('after_slim_index_build')
                connection.execute(text(f"SELECT {control}.update_file_result(:id,'STAGED',:raw,:canonical,:reject,NULL)"),
                                   {'id':int(f['etl_file_id']),'raw':actual['raw_count'],'canonical':actual['canonical_count'],'reject':actual['rejected_count']})
                details={'loader_mode':'direct','manifest_signature':receipt,**context['metrics']}
                connection.execute(text(f"SELECT {control}.finish_step(:batch,'STAGING','SUCCESS',0,CAST(:details AS jsonb),NULL)"),{'batch':batch,'details':json.dumps(details)})
                connection.execute(text(f'SELECT {control}.update_batch_counters(:batch,:raw,:canonical,:reject,0,0,0,0)'),
                                   {'batch':batch,'raw':actual['raw_count'],'canonical':actual['canonical_count'],'reject':actual['rejected_count']})
                connection.execute(text(f"SELECT {control}.transition_batch(:batch,'STAGED')"),{'batch':batch})
                checkpoint('before_commit')
            committed=True
            checkpoint('after_commit')
        except BaseException:
            if started and not committed:
                # Check for an uncertain COMMIT result before failing a completed batch.
                with writer.engine.connect() as check:
                    current=state(check,batch,control)
                if current and current['batch_status']=='RUNNING' and current['staging_status']=='RUNNING':
                    heartbeat(f"SELECT {control}.finish_step(%(batch)s,'STAGING','FAILED',1,'{{}}'::jsonb,'Direct staging failed; transaction rolled back'); SELECT {control}.fail_batch(%(batch)s,'STAGING_FAILED','Direct staging failed; transaction rolled back');",{'batch':batch})
            raise
        finally:
            if locked:
                connection.execute(text("SELECT pg_advisory_unlock(hashtext(:name))"),{'name':lock_name})
    return {**context,'stage_load_mode':'direct','direct_stage_published':True,'write_counts':counts,
            'raw_write_attempts':counts['raw'].get('attempted',0),
            'canonical_write_attempts':counts['canonical'].get('attempted',0)}


def iter_disk_parts(context):
    seen=set()
    for chunk in context['chunks']:
        raw=None
        if chunk['raw_path'] not in seen:
            with phase('read_raw_parquet'):
                raw=pd.read_parquet(chunk['raw_path'])
            if len(raw)!=int(chunk.get('source_part_rows',chunk['row_count'])):
                raise ValueError('Raw source-part count mismatch')
            seen.add(chunk['raw_path'])
        with phase('read_canonical_parquet'):
            canonical=pd.read_parquet(chunk['canonical_path'])
        if len(canonical)!=int(chunk['canonical_rows']): raise ValueError('Canonical part count mismatch')
        if int(chunk['technical_reject_rows']):
            with phase('read_reject_parquet'):
                rejected=pd.read_parquet(chunk['reject_path'])
            if len(rejected)!=int(chunk['technical_reject_rows']): raise ValueError('Reject part count mismatch')
        else:
            rejected=pd.DataFrame()
        yield raw,canonical,rejected,chunk
