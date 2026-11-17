from os import path

import pandas as pd
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def start_control_smoke_batch(*args, **kwargs):
    """Create and commit one disposable control batch."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    query = """
        SELECT batch_id, lset, cet
        FROM control.start_batch(
            'qa_43_t21_mage', 'yellow_taxi_tripdata', 'TEST',
            '1970-01-01 00:00:00+00'::timestamptz,
            NULL, 'mage_smoke_test', 'mage_container'
        );
    """
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        with loader.conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
        loader.conn.commit()

    if row is None:
        raise ValueError('start_batch returned no context')
    return pd.DataFrame([{
        'batch_id': int(row[0]),
        'lset': row[1],
        'cet': row[2],
    }])
