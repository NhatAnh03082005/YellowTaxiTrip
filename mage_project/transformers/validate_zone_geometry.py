from utils.profiling import profiled
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon
from utils.decorators import test, transformer


def _to_multipolygon(geometry):
    if isinstance(geometry, MultiPolygon):
        return geometry
    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])
    raise ValueError('Unsupported geometry type: {}'.format(geometry.geom_type))


@transformer
@profiled
def validate_zone_geometry(zone_lookup, zone_geometry, *args, **kwargs):
    lookup = zone_lookup.copy()
    if isinstance(zone_geometry, gpd.GeoDataFrame):
        geometry = zone_geometry.copy()
    else:
        required_transport_columns = {'geometry_wkt', 'source_crs_epsg'}
        missing_transport = required_transport_columns - set(zone_geometry.columns)
        if missing_transport:
            raise ValueError(
                'Geometry transport columns missing: {}'.format(
                    sorted(missing_transport)
                )
            )
        epsg_values = set(
            pd.to_numeric(zone_geometry['source_crs_epsg'], errors='coerce')
            .dropna().astype(int)
        )
        if epsg_values != {2263}:
            raise ValueError(
                'Expected source CRS EPSG:2263, found {}'.format(
                    sorted(epsg_values)
                )
            )
        attributes = zone_geometry.drop(
            columns=['geometry_wkt', 'source_crs_epsg']
        ).copy()
        attributes['geometry'] = gpd.GeoSeries.from_wkt(
            zone_geometry['geometry_wkt'], crs='EPSG:2263'
        )
        geometry = gpd.GeoDataFrame(
            attributes, geometry='geometry', crs='EPSG:2263'
        )
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
    validated = gpd.GeoDataFrame(
        geometry[['location_id', 'OBJECTID', 'Shape_Leng', 'Shape_Area', 'geometry']],
        geometry='geometry', crs='EPSG:2263',
    )
    # Mage 0.9.76 previews pandas outputs with to_json(orient='split').
    # GeoDataFrame overrides to_json without that argument, so pass WKT through
    # the block boundary and rebuild the GeoDataFrame in the exporter.
    output = pd.DataFrame(validated.drop(columns='geometry'))
    output['geometry_wkt'] = validated.geometry.to_wkt()
    return output


@test
def test_output(output, *args):
    assert len(output) == 263
    assert output['geometry_wkt'].notna().all()
    assert output['geometry_wkt'].str.startswith('MULTIPOLYGON').all()
