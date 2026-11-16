from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def test_postgres_connection(*args, **kwargs):
    config_path = path.join(get_repo_path(), 'io_config.yaml')

    query = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name IN (
            'stage',
            'control',
            'dq',
            'dds',
            'olap'
        )
        ORDER BY schema_name;
        """

    with Postgres.with_config(
        ConfigFileLoader(config_path, 'default')
    ) as loader:
        return loader.load(query)
