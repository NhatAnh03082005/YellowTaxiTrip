import csv
import unittest
from pathlib import Path

import geopandas as gpd

try:
    from mage_project.utils.parquet_utils import inspect_parquet
except ModuleNotFoundError:  # Mage container exposes the project as nyc_taxi_bi.
    from nyc_taxi_bi.utils.parquet_utils import inspect_parquet


HOST_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = (
    HOST_PROJECT_ROOT / 'data'
    if (HOST_PROJECT_ROOT / 'data').is_dir()
    else Path('/home/src/data')
)


class ProjectInputIntegrationTests(unittest.TestCase):
    def test_catalog_has_24_validated_files_on_disk(self):
        catalog_path = DATA_ROOT / 'catalog/yellow_taxi_2024_2025.csv'
        with catalog_path.open(encoding='utf-8', newline='') as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 24)
        self.assertEqual(len({row['source_year_month'] for row in rows}), 24)
        for row in rows:
            self.assertEqual(row['validation_status'], 'VALIDATED')
            relative = Path(row['local_path'])
            if relative.parts and relative.parts[0].lower() == 'data':
                relative = Path(*relative.parts[1:])
            self.assertTrue((DATA_ROOT / relative).is_file())

    def test_representative_2024_and_2025_schema_versions(self):
        registry_path = DATA_ROOT / 'catalog/yellow_taxi_schema_registry.csv'
        with registry_path.open(encoding='utf-8', newline='') as stream:
            registry = {
                row['schema_fingerprint']: row['schema_version']
                for row in csv.DictReader(stream)
            }
        cases = {
            'yellow_taxi/by_year_month/2024-05/yellow_tripdata_2024-05.parquet':
                'yt_schema_v001',
            'yellow_taxi/by_year_month/2025-05/yellow_tripdata_2025-05.parquet':
                'yt_schema_v002',
        }
        for relative_path, expected_version in cases.items():
            metadata = inspect_parquet(DATA_ROOT / relative_path)
            self.assertEqual(registry[metadata['schema_fingerprint']], expected_version)

    def test_reference_lookup_and_geometry_contract(self):
        lookup = DATA_ROOT / 'reference/taxi+_zone_lookup.csv'
        with lookup.open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        lookup_ids = {int(row['LocationID']) for row in rows}
        geometry = gpd.read_file(
            DATA_ROOT / 'reference/taxi_zones/taxi_zones.shp', engine='pyogrio'
        )
        geometry_ids = set(geometry['LocationID'].astype(int))
        self.assertEqual(len(rows), 265)
        self.assertEqual(len(geometry), 263)
        self.assertEqual(geometry.crs.to_epsg(), 2263)
        self.assertEqual(lookup_ids - geometry_ids, {264, 265})
        self.assertTrue(geometry.geometry.is_valid.all())


if __name__ == '__main__':
    unittest.main()
