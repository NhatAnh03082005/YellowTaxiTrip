from utils.profiling import profiled
from utils.database import execute, fetch_one

if 'transformer' not in globals():
    from utils.decorators import transformer


def _verify_stage(ingestion_context, *args, **kwargs):
    batch_id = int(ingestion_context['batch_id'])
    metrics = ingestion_context['metrics']
    raw_count = canonical_count = rejected_count = 0
    for item in ingestion_context['files']:
        params = {'batch': batch_id,'version':int(item['file_version_id']),
                  'sha':item['source_file_sha256'],'file':item['file_name'],
                  'expected':int(item['parquet_row_count']), 'schema':item['schema_version'],
                  'fingerprint':item['schema_fingerprint'],'month':item['source_year_month']}
        counts = validate_file_stage(params, ingestion_context['chunks'])
        raw_count += counts['raw_count']
        canonical_count += counts['canonical_count']
        rejected_count += counts['rejected_count']
    if (raw_count, canonical_count, rejected_count) != (
            metrics['rows_raw'],metrics['rows_canonical'],metrics['rows_technical_quarantined']):
        raise ValueError('Stage actual counts differ from local metrics')
    execute(
        """
        SELECT control.finish_step(
            %(batch_id)s, 'STAGING', 'SUCCESS', 0,
            jsonb_build_object('rows_raw', %(raw)s,
                               'rows_canonical', %(canonical)s,
                               'rows_technical_quarantined', %(rejected)s), NULL
        );
        SELECT control.update_batch_counters(
            %(batch_id)s, %(raw)s, %(canonical)s, %(rejected)s, 0, 0, 0, 0
        );
        SELECT control.transition_batch(%(batch_id)s, 'STAGED');
        """,
        {'batch_id': batch_id, 'raw': raw_count, 'canonical': canonical_count,
         'rejected': rejected_count},
    )
    return {
        'batch_id': batch_id, 'status': 'STAGED', 'rows_raw': raw_count,
        'rows_canonical': canonical_count,
        'rows_technical_quarantined': rejected_count,
        'local_metrics': metrics,
    }


def validate_file_stage(params, chunks, reader=None):
    expected = params['expected']
    local = [c for c in chunks if c['source_file'] == params['file']]
    local_counts = (sum(c['row_count'] for c in local),sum(c['canonical_rows'] for c in local),sum(c['technical_reject_rows'] for c in local))
    if local_counts[0] != expected or local_counts[0] != local_counts[1]+local_counts[2]:
        raise ValueError('Source/footer/local manifest counts mismatch')
    # Include all rows for this SHA to detect stale/wrong-scope retries rather than hiding them.
    bad_scope = """batch_id IS DISTINCT FROM %(batch)s
        OR file_version_id IS DISTINCT FROM %(version)s OR source_file IS DISTINCT FROM %(file)s
        OR schema_version IS DISTINCT FROM %(schema)s OR schema_fingerprint IS DISTINCT FROM %(fingerprint)s
        OR source_year_month IS DISTINCT FROM %(month)s
        OR source_row_number IS NULL OR source_row_number<1 OR source_row_number>%(expected)s
        OR source_record_key IS DISTINCT FROM
          encode(sha256(convert_to(lower(%(sha)s)::text || ':' || source_row_number::text,'UTF8')),'hex')"""
    branches=[]
    for kind,table in [('raw','stage.yellow_taxi_raw'),('canonical','stage.yellow_taxi_canonical'),('reject','dq.technical_quarantine')]:
        branches.append("SELECT '{}' kind,source_record_key,source_row_number,({}) bad_scope FROM {} WHERE source_file_sha256=%(sha)s".format(kind,bad_scope,table))
    # Materialize only identity + one checked boolean, not repeated wide metadata strings.
    query="""
        WITH rows AS ("""+' UNION ALL '.join(branches)+"""), key_counts AS (
          SELECT source_record_key,count(*) FILTER(WHERE kind='raw') r,
            count(*) FILTER(WHERE kind='canonical') c,count(*) FILTER(WHERE kind='reject') q
          FROM rows GROUP BY source_record_key
        )
        SELECT count(*) FILTER(WHERE kind='raw') raw_count,
          count(*) FILTER(WHERE kind='canonical') canonical_count,
          count(*) FILTER(WHERE kind='reject') rejected_count,
          count(DISTINCT source_row_number) FILTER(WHERE kind='raw') distinct_rows,
          count(*) FILTER(WHERE bad_scope) bad_scope,
          (SELECT count(*) FROM key_counts WHERE r<>1 OR c+q<>1) bad_partition
        FROM rows
        """
    result = (reader or fetch_one)(query,params)
    actual = (result['raw_count'],result['canonical_count'],result['rejected_count'])
    if actual != local_counts or result['distinct_rows'] != expected or result['bad_scope'] or result['bad_partition']:
        raise ValueError('Stage count/key/identity partition validation failed: {}'.format(result))
    return result


def validate_unified_file_stage(params, chunks, reader=None, stage='stage', dq='dq', control='control'):
    """Validate slim-v2 with one materialized physical scan.

    Raw is every physical row, Canonical is ``technical_valid`` and technical
    quarantine is its exact complement. This preserves the original DQ
    partition checks without scanning two full physical trip copies.
    """
    expected = params['expected']
    local = [c for c in chunks if c['source_file'] == params['file']]
    local_counts = (
        sum(c['row_count'] for c in local),
        sum(c['canonical_rows'] for c in local),
        sum(c['technical_reject_rows'] for c in local),
    )
    if local_counts[0] != expected or local_counts[0] != local_counts[1] + local_counts[2]:
        raise ValueError('Source/footer/local manifest counts mismatch')
    query = f"""
      WITH base AS MATERIALIZED (
        SELECT d.source_record_key,d.source_row_number,d.technical_valid,
          (m.batch_id IS DISTINCT FROM %(batch)s
           OR d.file_version_id IS DISTINCT FROM %(version)s
           OR v.source_file IS DISTINCT FROM %(file)s
           OR v.schema_version IS DISTINCT FROM %(schema)s
           OR v.schema_fingerprint IS DISTINCT FROM %(fingerprint)s
           OR v.source_year_month IS DISTINCT FROM %(month)s
           OR d.source_row_number IS NULL OR d.source_row_number<1
           OR d.source_row_number>%(expected)s
           OR d.source_record_key IS DISTINCT FROM
             encode(sha256(convert_to(lower(%(sha)s)::text || ':' || d.source_row_number::text,'UTF8')),'hex')
          ) bad_scope
        FROM {stage}.yellow_taxi_trip_data d
        JOIN {stage}.stage_file_batch m USING(file_version_id)
        JOIN {control}.source_file_version v USING(file_version_id)
        WHERE v.source_file_sha256=%(sha)s
      ), quarantine AS MATERIALIZED (
        SELECT source_record_key,count(*) AS copies
        FROM {dq}.technical_quarantine
        WHERE source_file_sha256=%(sha)s
        GROUP BY source_record_key
      )
      SELECT count(*) raw_count,
        count(*) FILTER(WHERE technical_valid) canonical_count,
        count(*) FILTER(WHERE NOT technical_valid) rejected_count,
        count(DISTINCT source_row_number) distinct_rows,
        count(*) FILTER(WHERE bad_scope) bad_scope,
        (SELECT count(*) FROM base b FULL JOIN quarantine q USING(source_record_key)
          WHERE b.source_record_key IS NULL
             OR (b.technical_valid AND q.source_record_key IS NOT NULL)
             OR (NOT b.technical_valid AND (q.source_record_key IS NULL OR q.copies<>1))) bad_partition
      FROM base
    """
    result = (reader or fetch_one)(query, params)
    actual = (result['raw_count'], result['canonical_count'], result['rejected_count'])
    if (actual != local_counts or result['distinct_rows'] != expected
            or result['bad_scope'] or result['bad_partition']):
        raise ValueError('Unified stage count/key/identity partition validation failed: {}'.format(result))
    return result


@transformer
@profiled
def verify_stage(ingestion_context, *args, **kwargs):
    try:
        if ingestion_context.get('direct_stage_published'):
            from utils.atomic_stage import verify_published
            return verify_published(ingestion_context)
        return _verify_stage(ingestion_context, *args, **kwargs)
    except Exception as exception:
        if ingestion_context.get('direct_stage_published'):
            execute("SELECT control.fail_batch(%(batch)s,'STAGING_VALIDATION_FAILED',%(message)s);",
                    {'batch':int(ingestion_context['batch_id']),'message':str(exception)[:2000]})
            raise
        execute("""
            SELECT control.finish_step(%(batch)s,'STAGING','FAILED',1,'{}'::jsonb,%(message)s);
            SELECT control.fail_batch(%(batch)s,'STAGING_VALIDATION_FAILED',%(message)s);
        """,{'batch':int(ingestion_context['batch_id']),'message':str(exception)[:2000]})
        raise
