import csv
import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

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

    def test_reference_loader_preserves_literal_na_members(self):
        lookup = pd.read_csv(
            DATA_ROOT / 'reference/taxi+_zone_lookup.csv',
            dtype={'LocationID': 'Int64', 'Borough': 'string', 'Zone': 'string'},
            keep_default_na=False,
        )
        row_264 = lookup.loc[lookup['LocationID'] == 264].iloc[0]
        row_265 = lookup.loc[lookup['LocationID'] == 265].iloc[0]
        self.assertEqual(row_264['Borough'], 'Unknown')
        self.assertEqual(row_264['Zone'], 'N/A')
        self.assertEqual(row_265['Borough'], 'N/A')
        self.assertEqual(row_265['Zone'], 'Outside of NYC')

    def test_geometry_transform_output_is_mage_preview_safe(self):
        try:
            from mage_project.data_loaders.load_zone_geometry import load_zone_geometry
            from mage_project.transformers.validate_zone_lookup import validate_zone_lookup
            from mage_project.transformers.validate_zone_geometry import validate_zone_geometry
        except ModuleNotFoundError:
            from nyc_taxi_bi.data_loaders.load_zone_geometry import load_zone_geometry
            from nyc_taxi_bi.transformers.validate_zone_lookup import validate_zone_lookup
            from nyc_taxi_bi.transformers.validate_zone_geometry import validate_zone_geometry

        lookup_raw = pd.read_csv(
            DATA_ROOT / 'reference/taxi+_zone_lookup.csv',
            dtype={'LocationID': 'Int64', 'Borough': 'string', 'Zone': 'string',
                   'service_zone': 'string'},
            keep_default_na=False,
        )
        lookup = validate_zone_lookup(lookup_raw)
        geometry_raw = load_zone_geometry(
            zone_shapefile_path=str(
                DATA_ROOT / 'reference/taxi_zones/taxi_zones.shp'
            )
        )
        self.assertIsInstance(geometry_raw, pd.DataFrame)
        self.assertNotIsInstance(geometry_raw, gpd.GeoDataFrame)
        self.assertTrue(geometry_raw.to_json(orient='split'))
        output = validate_zone_geometry(lookup, geometry_raw)
        self.assertIsInstance(output, pd.DataFrame)
        self.assertNotIsInstance(output, gpd.GeoDataFrame)
        self.assertTrue(output['geometry_wkt'].str.startswith('MULTIPOLYGON').all())
        # This is the exact preview operation that failed on GeoDataFrame.
        self.assertTrue(output.to_json(orient='split'))

    def test_downstream_blocks_accept_mage_context_keyword(self):
        try:
            from mage_project.transformers.load_source_files import load_source_files
            from mage_project.transformers.canonicalize_schema import canonicalize_schema
            from mage_project.transformers.standardize_and_validate import (
                standardize_and_validate,
            )
        except ModuleNotFoundError:
            from nyc_taxi_bi.transformers.load_source_files import load_source_files
            from nyc_taxi_bi.transformers.canonicalize_schema import canonicalize_schema
            from nyc_taxi_bi.transformers.standardize_and_validate import (
                standardize_and_validate,
            )

        with tempfile.TemporaryDirectory() as temporary:
            runtime_context = {'runtime': 'mage-reserved-keyword'}
            loaded = load_source_files(
                {'batch_id': 999, 'files': []},
                context=runtime_context,
                work_root=temporary,
            )
            canonical = canonicalize_schema(
                loaded,
                context=runtime_context,
                work_root=temporary,
            )
            validated = standardize_and_validate(
                canonical,
                context=runtime_context,
            )
            self.assertEqual(validated['metrics']['rows_raw'], 0)

    def test_source_loader_uses_bounded_parts_and_stable_lineage(self):
        try:
            from mage_project.transformers.load_source_files import load_source_files
        except ModuleNotFoundError:
            from nyc_taxi_bi.transformers.load_source_files import load_source_files

        with tempfile.TemporaryDirectory() as temporary:
            source_path = Path(temporary) / 'source.parquet'
            pd.DataFrame({
                'tpep_pickup_datetime': pd.date_range(
                    '2025-01-01', periods=5, freq='min'
                ),
                'total_amount': [1.0, 2.0, 3.0, 4.0, 5.0],
            }).to_parquet(source_path, index=False)
            file_sha256 = 'a' * 64
            loaded = load_source_files(
                {
                    'batch_id': 1001,
                    'files': [{
                        'resolved_path': str(source_path),
                        'file_name': source_path.name,
                        'source_year_month': '2025-01',
                        'source_file_sha256': file_sha256,
                        'schema_fingerprint': 'fingerprint',
                        'schema_version': 'v001',
                        'last_modified': '2025-01-01T00:00:00+00:00',
                        'parquet_row_count': 5,
                        'etl_file_id': 1,
                    }],
                },
                work_root=temporary,
                source_batch_size=2,
            )
            self.assertEqual([part['row_count'] for part in loaded['chunks']], [2, 2, 1])
            combined = pd.concat([
                pq.read_table(part['raw_path']).to_pandas()
                for part in loaded['chunks']
            ], ignore_index=True)
            self.assertEqual(combined['source_row_number'].tolist(), [1, 2, 3, 4, 5])
            self.assertEqual(combined['source_record_key'].nunique(), 5)
            self.assertTrue((combined['batch_id'] == 1001).all())


if __name__ == '__main__':
    unittest.main()
