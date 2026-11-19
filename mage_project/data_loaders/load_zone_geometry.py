from pathlib import Path

import geopandas as gpd
import pandas as pd

if 'data_loader' not in globals():
    from utils.decorators import data_loader, test


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
    geometry = gpd.read_file(shp_path, engine='pyogrio')
    if geometry.crs is None:
        raise ValueError('Shapefile CRS is missing')
    output = pd.DataFrame(geometry.drop(columns='geometry'))
    output['geometry_wkt'] = geometry.geometry.to_wkt()
    output['source_crs_epsg'] = geometry.crs.to_epsg()
    return output


@test
def test_output(output, *args):
    assert output is not None
    assert len(output) == 263
    assert output['geometry_wkt'].notna().all()
    assert set(output['source_crs_epsg']) == {2263}
