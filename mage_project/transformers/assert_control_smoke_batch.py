from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def assert_control_smoke_batch(batch_context, *args, **kwargs):
    """Assert the downstream block sees the exact upstream batch_id."""
    batch_id = int(batch_context.iloc[0]['batch_id'])
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    query = f"""
        SELECT batch_id, pipeline_name, status
        FROM control.etl_batch
        WHERE batch_id = {batch_id};
    """
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        result = loader.load(query)

    if len(result.index) != 1:
        raise ValueError(f'Batch {batch_id} was not committed by the upstream block')
    if int(result.iloc[0]['batch_id']) != batch_id:
        raise ValueError('Downstream block received a different batch_id')
    if result.iloc[0]['pipeline_name'] != 'qa_43_t21_mage':
        raise ValueError('Unexpected pipeline_name')
    return batch_context
