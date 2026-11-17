import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def _to_multipolygon(geometry):
    if isinstance(geometry, MultiPolygon):
        return geometry
    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])
    raise ValueError('Unsupported geometry type: {}'.format(geometry.geom_type))


@transformer
def validate_zone_geometry(zone_lookup, zone_geometry, *args, **kwargs):
    lookup = zone_lookup.copy()
    geometry = zone_geometry.copy()
    if geometry.crs is None or geometry.crs.to_epsg() != 2263:
        raise ValueError('Expected source CRS EPSG:2263, found {}'.format(geometry.crs))
    geometry['location_id'] = pd.to_numeric(
        geometry['LocationID'], errors='coerce'
    ).astype('Int64')
    if geometry['location_id'].isna().any() or not geometry['location_id'].is_unique:
        raise ValueError('Geometry LocationID must be non-null and unique')
    if geometry.geometry.isna().any() or geometry.geometry.is_empty.any():
        raise ValueError('Geometry must be non-null and non-empty')
    if not geometry.geometry.is_valid.all():
        raise ValueError('Invalid geometry detected')
    unsupported = set(geometry.geometry.geom_type) - {'Polygon', 'MultiPolygon'}
    if unsupported:
        raise ValueError('Unsupported geometry types: {}'.format(sorted(unsupported)))
    lookup_ids = set(lookup['location_id'].astype(int))
    geometry_ids = set(geometry['location_id'].astype(int))
    if geometry_ids - lookup_ids:
        raise ValueError('Geometry IDs absent from lookup: {}'.format(
            sorted(geometry_ids - lookup_ids)
        ))
    if lookup_ids - geometry_ids != {264, 265}:
        raise ValueError('Expected only LocationID 264 and 265 without geometry')
    geometry['geometry'] = geometry.geometry.map(_to_multipolygon)
    return gpd.GeoDataFrame(
        geometry[['location_id', 'OBJECTID', 'Shape_Leng', 'Shape_Area', 'geometry']],
        geometry='geometry', crs='EPSG:2263',
    )


@test
def test_output(output, *args):
    assert len(output) == 263
    assert output.crs.to_epsg() == 2263
    assert set(output.geometry.geom_type) == {'MultiPolygon'}
