import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from mage_project.utils.canonical import canonicalize_dataframe, serialize_json_columns
    from mage_project.utils.parquet_utils import (
        calculate_file_sha256, inspect_parquet, make_source_record_key,
    )
    from mage_project.utils.paths import resolve_data_path
except ModuleNotFoundError:  # Mage container exposes the project as nyc_taxi_bi.
    from nyc_taxi_bi.utils.canonical import canonicalize_dataframe, serialize_json_columns
    from nyc_taxi_bi.utils.parquet_utils import (
        calculate_file_sha256, inspect_parquet, make_source_record_key,
    )
    from nyc_taxi_bi.utils.paths import resolve_data_path


class PathTests(unittest.TestCase):
    def test_catalog_data_prefix_is_mapped_below_mount(self):
        with tempfile.TemporaryDirectory() as temporary:
            resolved = resolve_data_path(
                'data/yellow_taxi/file.parquet', data_root=temporary
            )
            self.assertEqual(
                resolved,
                Path(temporary).resolve() / 'yellow_taxi' / 'file.parquet',
            )

    def test_path_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                resolve_data_path('../outside.txt', data_root=temporary)


class ParquetUtilityTests(unittest.TestCase):
    def _write_valid_parquet(self, path):
        table = pa.table({
            'VendorID': [1],
            'tpep_pickup_datetime': [pd.Timestamp('2024-05-01 10:00:00')],
            'tpep_dropoff_datetime': [pd.Timestamp('2024-05-01 10:10:00')],
            'PULocationID': [132],
            'DOLocationID': [138],
            'total_amount': [25.50],
        })
        pq.write_table(table, path)

    def test_sha256_reads_file_in_chunks(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'payload.bin'
            path.write_bytes(b'abc' * 100)
            self.assertEqual(
                calculate_file_sha256(path, chunk_size=7),
                hashlib.sha256(path.read_bytes()).hexdigest(),
            )

    def test_schema_fingerprint_uses_order_name_and_logical_type(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'valid.parquet'
            self._write_valid_parquet(path)
            result = inspect_parquet(path)
            serialized = json.dumps(
                result['schema_contract'], ensure_ascii=False, separators=(',', ':')
            )
            self.assertEqual(
                result['schema_fingerprint'],
                hashlib.sha256(serialized.encode('utf-8')).hexdigest(),
            )
            self.assertEqual(result['row_count'], 1)

    def test_source_record_key_is_stable_and_position_sensitive(self):
        sha = 'a' * 64
        self.assertEqual(make_source_record_key(sha, 1), make_source_record_key(sha, 1))
        self.assertNotEqual(make_source_record_key(sha, 1), make_source_record_key(sha, 2))


class CanonicalizationTests(unittest.TestCase):
    def _base_rows(self):
        return pd.DataFrame({
            'VendorID': [1, 2],
            'tpep_pickup_datetime': ['2024-05-01 10:00:00', 'not-a-time'],
            'tpep_dropoff_datetime': ['2024-05-01 10:10:00', '2024-05-01 11:00:00'],
            'passenger_count': [1, 2],
            'trip_distance': [2.5, 3.0],
            'RatecodeID': [1, 1],
            'store_and_fwd_flag': ['n', 'unexpected'],
            'PULocationID': [132, 132],
            'DOLocationID': [138, 138],
            'payment_type': [1, 1],
            'fare_amount': [20.0, 21.0],
            'total_amount': [25.0, 26.0],
            'Airport_fee': [1.75, 1.75],
            'source_row_number': [1, 2],
            'source_record_key': ['1' * 64, '2' * 64],
            'source_file_sha256': ['a' * 64, 'a' * 64],
            'source_file': ['file.parquet', 'file.parquet'],
            'source_year_month': ['2024-05', '2024-05'],
            'schema_fingerprint': ['b' * 64, 'b' * 64],
            'schema_version': ['yt_schema_v001', 'yt_schema_v001'],
            'batch_id': [10, 10],
        })

    def test_valid_row_is_canonical_and_invalid_timestamp_is_rejected(self):
        accepted, rejected = canonicalize_dataframe(self._base_rows())
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(accepted.iloc[0]['airport_fee'], 1.75)
        self.assertTrue(pd.isna(accepted.iloc[0]['cbd_congestion_fee']))
        self.assertEqual(accepted.iloc[0]['store_and_fwd_flag'], 'N')
        self.assertIn('pickup_datetime', rejected.iloc[0]['cast_error_flags'])
        self.assertEqual(len(accepted) + len(rejected), 2)

    def test_business_anomaly_is_not_a_technical_reject(self):
        dataframe = self._base_rows().iloc[[0]].copy()
        dataframe['total_amount'] = -10.0
        dataframe['trip_distance'] = -1.0
        accepted, rejected = canonicalize_dataframe(dataframe)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 0)

    def test_json_columns_can_be_written_when_error_dictionary_is_empty(self):
        dataframe = self._base_rows().iloc[[0]].copy()
        accepted, rejected = canonicalize_dataframe(dataframe)
        with tempfile.TemporaryDirectory() as temporary:
            accepted_path = Path(temporary) / 'accepted.parquet'
            rejected_path = Path(temporary) / 'rejected.parquet'
            serialize_json_columns(accepted).to_parquet(accepted_path, index=False)
            serialize_json_columns(rejected).to_parquet(rejected_path, index=False)
            self.assertEqual(len(pd.read_parquet(accepted_path)), 1)
            self.assertEqual(len(pd.read_parquet(rejected_path)), 0)


if __name__ == '__main__':
    unittest.main()
