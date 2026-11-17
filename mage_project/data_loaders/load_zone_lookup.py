from pathlib import Path

import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_zone_lookup(*args, **kwargs):
    csv_path = Path(kwargs.get(
        'zone_csv_path', '/home/src/data/reference/taxi+_zone_lookup.csv'
    ))
    if not csv_path.is_file():
        raise FileNotFoundError('Zone lookup not found: {}'.format(csv_path))
    return pd.read_csv(csv_path, dtype={
        'LocationID': 'Int64', 'Borough': 'string',
        'Zone': 'string', 'service_zone': 'string',
    })


@test
def test_output(output, *args):
    assert output is not None
    assert len(output) == 265
