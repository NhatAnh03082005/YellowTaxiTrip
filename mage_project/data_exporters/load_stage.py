from importlib import reload

import pandas as pd

from utils.database import execute, fetch_one, require_tables
import utils.stage_loader as stage_loader
if 'data_exporter' not in globals():
    from utils.decorators import data_exporter

@data_exporter
def load_stage(ingestion_context, *args, **kwargs):
    # Mage keeps utility imports cached between manual block reruns. Reload the
    # module so operational fixes are used without rebuilding upstream outputs.
    reload(stage_loader)
    upsert_dataframe = stage_loader.upsert_dataframe
    batch_id = int(ingestion_context['batch_id'])
    require_tables(['stage.yellow_taxi_raw', 'stage.yellow_taxi_canonical'])
    control_state = fetch_one(
        """
        SELECT b.status AS batch_status, b.error_code,
               s.status AS staging_status
        FROM control.etl_batch b
        JOIN control.etl_step_run s
          ON s.batch_id = b.batch_id AND s.step_name = 'STAGING'
        WHERE b.batch_id = %(batch_id)s;
        """,
        {'batch_id': batch_id},
    )
    if (control_state
            and control_state['batch_status'] == 'FAILED'
            and control_state['staging_status'] == 'FAILED'):
        execute(
            'SELECT control.resume_failed_staging(%(batch_id)s);',
            {'batch_id': batch_id},
        )
    execute("SELECT control.start_step(%(batch_id)s, 'STAGING');", {'batch_id': batch_id})
    raw_attempts = 0
    canonical_attempts = 0
    try:
        for chunk_number, chunk in enumerate(ingestion_context['chunks'], start=1):
            raw = pd.read_parquet(chunk['raw_path'])
            canonical = pd.read_parquet(chunk['canonical_path'])
            raw_attempts += upsert_dataframe(raw, 'stage', 'yellow_taxi_raw')
            canonical_attempts += upsert_dataframe(
                canonical, 'stage', 'yellow_taxi_canonical'
            )
            if chunk_number % 10 == 0:
                execute(
                    'SELECT control.touch_batch_heartbeat(%(batch_id)s);',
                    {'batch_id': batch_id},
                )
                print(
                    'Staged {}/{} bounded parts'.format(
                        chunk_number, len(ingestion_context['chunks'])
                    )
                )
        grouped = {}
        for chunk in ingestion_context['chunks']:
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
        **ingestion_context, 'raw_write_attempts': raw_attempts,
        'canonical_write_attempts': canonical_attempts,
    }
