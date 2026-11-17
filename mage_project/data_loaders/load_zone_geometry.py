from pathlib import Path

import geopandas as gpd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_zone_geometry(*args, **kwargs):
    shp_path = Path(kwargs.get(
        'zone_shapefile_path',
        '/home/src/data/reference/taxi_zones/taxi_zones.shp',
    ))
    missing = [
        str(shp_path.with_suffix(extension))
        for extension in ('.shp', '.shx', '.dbf', '.prj')
        if not shp_path.with_suffix(extension).is_file()
    ]
    if missing:
        raise FileNotFoundError('Missing Shapefile components: {}'.format(missing))
    return gpd.read_file(shp_path, engine='pyogrio')


@test
def test_output(output, *args):
    assert output is not None
    assert len(output) == 263
