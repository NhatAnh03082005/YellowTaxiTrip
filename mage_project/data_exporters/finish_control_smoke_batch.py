from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def finish_control_smoke_batch(batch_context, *args, **kwargs):
    """Close the smoke batch without advancing its watermark."""
    batch_id = int(batch_context.iloc[0]['batch_id'])
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    query = f"""
        SELECT control.fail_batch(
            {batch_id}, 'MAGE_SMOKE_TEST_COMPLETED',
            'Mage propagated one batch_id through all smoke-test blocks'
        );
    """
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        loader.execute_query_raw(query)
    return {'batch_id': batch_id, 'status': 'FAILED_AS_EXPECTED'}
