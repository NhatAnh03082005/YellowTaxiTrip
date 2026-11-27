from utils.profiling import profiled
from utils.database import fetch_one

if 'data_loader' not in globals():
    from utils.decorators import data_loader
if 'test' not in globals():
    from utils.decorators import test


@data_loader
@profiled
def get_extraction_window(*args, **kwargs):
    run_type = kwargs.get('run_type', 'BACKFILL').upper()
    parent_batch_id = kwargs.get('parent_batch_id')
    if run_type not in {'BACKFILL', 'INCREMENTAL', 'RETRY', 'TEST'}:
        raise ValueError('Unsupported run_type: {}'.format(run_type))
    if run_type == 'RETRY' and parent_batch_id is None:
        raise ValueError('RETRY requires parent_batch_id')
    storage=kwargs.get('stage_storage_mode','wide')
    prepare=kwargs.get('prepare_mode','disk')
    mode=kwargs.get('stage_load_mode','upsert')
    if storage not in ('wide','slim') or prepare not in ('disk','streaming') or mode not in ('upsert','direct'):
        raise ValueError('Invalid Mage preparation/storage/loading mode')
    if (storage=='slim' or prepare=='streaming') and mode!='direct':
        raise ValueError('Slim/streaming require direct mode before creating a batch')
    marker=fetch_one("SELECT to_regclass('stage.stage_storage_contract') AS relation;")
    if marker and marker['relation'] is not None:
        installed=fetch_one('SELECT storage_mode FROM stage.stage_storage_contract;')
        if (not installed or installed['storage_mode'] not in ('slim-v1','slim-v2')
                or storage!='slim'):
            raise RuntimeError('Installed staging contract requires matching slim mode; no batch was created')
    elif storage=='slim':
        raise RuntimeError('Slim staging migration is not installed; no batch was created')
    row = fetch_one(
        """
        SELECT batch_id, lset, cet
        FROM control.start_batch(
            %(pipeline_name)s, %(source_name)s, %(run_type)s,
            %(initial_lset)s::timestamptz, %(parent_batch_id)s,
            %(triggered_by)s, %(worker_id)s
        );
        """,
        {
            'pipeline_name': 'pl_taxi_master',
            'source_name': 'yellow_taxi_tripdata',
            'run_type': run_type,
            'initial_lset': '1970-01-01 00:00:00+00',
            'parent_batch_id': parent_batch_id,
            'triggered_by': kwargs.get('triggered_by', 'mage'),
            'worker_id': kwargs.get('worker_id', 'mage_container'),
        },
    )
    if not row:
        raise RuntimeError('control.start_batch returned no context')
    return {
        'batch_id': int(row['batch_id']), 'lset': row['lset'], 'cet': row['cet'],
        'run_type': run_type, 'pipeline_name': 'pl_taxi_master',
        'source_name': 'yellow_taxi_tripdata',
    }


@test
def test_output(output, *args):
    assert output['batch_id'] > 0
    assert output['lset'] is not None and output['cet'] is not None
