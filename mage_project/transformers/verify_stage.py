from utils.database import execute, fetch_one

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def verify_stage(ingestion_context, *args, **kwargs):
    batch_id = int(ingestion_context['batch_id'])
    hashes = sorted({
        item['source_file_sha256'] for item in ingestion_context['files']
    })
    metrics = ingestion_context['metrics']
    if not hashes:
        raw_count = canonical_count = rejected_count = 0
    else:
        raw_count = fetch_one(
            'SELECT count(*) AS count FROM stage.yellow_taxi_raw '
            'WHERE source_file_sha256 = ANY(%(hashes)s);', {'hashes': hashes}
        )['count']
        canonical_count = fetch_one(
            'SELECT count(*) AS count FROM stage.yellow_taxi_canonical '
            'WHERE source_file_sha256 = ANY(%(hashes)s);', {'hashes': hashes}
        )['count']
        rejected_count = fetch_one(
            'SELECT count(*) AS count FROM dq.technical_quarantine '
            'WHERE source_file_sha256 = ANY(%(hashes)s);', {'hashes': hashes}
        )['count']
    if raw_count != canonical_count + rejected_count:
        raise ValueError(
            'Stage reconciliation failed: raw={} canonical={} rejected={}'.format(
                raw_count, canonical_count, rejected_count
            )
        )
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
