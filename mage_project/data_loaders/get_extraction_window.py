from utils.database import fetch_one
from utils.decorators import data_loader, test


@data_loader
def get_extraction_window(*args, **kwargs):
    run_type = kwargs.get('run_type', 'BACKFILL').upper()
    parent_batch_id = kwargs.get('parent_batch_id')
    if run_type not in {'BACKFILL', 'INCREMENTAL', 'RETRY', 'TEST'}:
        raise ValueError('Unsupported run_type: {}'.format(run_type))
    if run_type == 'RETRY' and parent_batch_id is None:
        raise ValueError('RETRY requires parent_batch_id')
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
