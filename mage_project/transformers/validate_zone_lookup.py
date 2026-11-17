import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def validate_zone_lookup(dataframe, *args, **kwargs):
    required = {'LocationID', 'Borough', 'Zone', 'service_zone'}
    missing = required - set(dataframe.columns)
    if missing:
        raise ValueError('Missing lookup columns: {}'.format(sorted(missing)))
    result = dataframe.copy()
    result['location_id'] = pd.to_numeric(
        result['LocationID'], errors='coerce'
    ).astype('Int64')
    result['borough'] = result['Borough'].str.strip()
    result['zone_name'] = result['Zone'].str.strip()
    result['service_zone'] = result['service_zone'].str.strip()
    if len(result) != 265:
        raise ValueError('Expected 265 lookup rows, found {}'.format(len(result)))
    if result['location_id'].isna().any() or not result['location_id'].is_unique:
        raise ValueError('LocationID must be non-null and unique')
    actual = set(result['location_id'].astype(int))
    expected = set(range(1, 266))
    if actual != expected:
        raise ValueError('LocationID coverage mismatch: missing={}, extra={}'.format(
            sorted(expected - actual), sorted(actual - expected)
        ))
    if result[['borough', 'zone_name']].isna().any().any():
        raise ValueError('Borough and zone name must be populated')
    return result[['location_id', 'borough', 'zone_name', 'service_zone']]


@test
def test_output(output, *args):
    assert len(output) == 265
    assert output['location_id'].nunique() == 265
