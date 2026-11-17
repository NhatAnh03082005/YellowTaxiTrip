import pandas as pd

from utils.database import execute, require_tables
from utils.stage_loader import upsert_dataframe

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def load_stage(context, *args, **kwargs):
    batch_id = int(context['batch_id'])
    require_tables(['stage.yellow_taxi_raw', 'stage.yellow_taxi_canonical'])
    execute("SELECT control.start_step(%(batch_id)s, 'STAGING');", {'batch_id': batch_id})
    raw_attempts = 0
    canonical_attempts = 0
    try:
        for chunk in context['chunks']:
            raw = pd.read_parquet(chunk['raw_path'])
            canonical = pd.read_parquet(chunk['canonical_path'])
            raw_attempts += upsert_dataframe(raw, 'stage', 'yellow_taxi_raw')
            canonical_attempts += upsert_dataframe(
                canonical, 'stage', 'yellow_taxi_canonical'
            )
        grouped = {}
        for chunk in context['chunks']:
            item = grouped.setdefault(chunk['etl_file_id'], {
                'read': 0, 'canonical': 0, 'rejected': 0,
            })
            item['read'] += chunk['row_count']
            item['canonical'] += chunk['canonical_rows']
            item['rejected'] += chunk['technical_reject_rows']
        for etl_file_id, counts in grouped.items():
            execute(
                """
                SELECT control.update_file_result(
                    %(id)s, 'STAGED', %(read)s, %(canonical)s, %(rejected)s, NULL
                );
                """,
                {'id': etl_file_id, **counts},
            )
    except Exception as exception:
        execute(
            """
            SELECT control.finish_step(%(batch_id)s, 'STAGING', 'FAILED', 1,
                                       '{}'::jsonb, %(message)s);
            SELECT control.fail_batch(%(batch_id)s, 'STAGING_FAILED', %(message)s);
            """,
            {'batch_id': batch_id, 'message': str(exception)[:2000]},
        )
        raise
    return {
        **context, 'raw_write_attempts': raw_attempts,
        'canonical_write_attempts': canonical_attempts,
    }
