import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry

from utils.database import require_tables

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def _engine():
    password = quote_plus(os.environ['POSTGRES_PASSWORD'])
    url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        os.environ['POSTGRES_USER'], password, os.environ['POSTGRES_HOST'],
        os.environ.get('POSTGRES_PORT', '5432'), os.environ['POSTGRES_DB'],
    )
    return create_engine(url)


@data_exporter
def load_reference_stage(zone_lookup, zone_geometry, *args, **kwargs):
    require_tables(['stage.taxi_zone_raw', 'stage.taxi_zone_geometry'])
    engine = _engine()
    with engine.begin() as connection:
        connection.execute(text('TRUNCATE TABLE stage.taxi_zone_geometry;'))
        connection.execute(text('TRUNCATE TABLE stage.taxi_zone_raw;'))
        zone_lookup.to_sql(
            'taxi_zone_raw', connection, schema='stage', if_exists='append', index=False,
            method='multi', chunksize=1000,
        )
        zone_geometry.to_postgis(
            'taxi_zone_geometry', connection, schema='stage', if_exists='append', index=False,
            dtype={'geometry': Geometry('MULTIPOLYGON', srid=2263)}, chunksize=500,
        )
    return {'lookup_rows': len(zone_lookup), 'geometry_rows': len(zone_geometry)}
