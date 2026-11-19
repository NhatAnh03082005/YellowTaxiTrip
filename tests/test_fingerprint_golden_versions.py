"""Golden v001/v002 fingerprint vectors executed through the dbt model SQL."""
from pathlib import Path
import subprocess
import unittest

import jinja2


class FingerprintGoldenVersions(unittest.TestCase):
    def test_2024_and_2025_hashes_are_stable(self):
        root = Path(__file__).resolve().parents[1]
        dbt = root / 'dbt_project'
        env = jinja2.Environment()
        macros = env.from_string((dbt / 'macros/fingerprint_fields.sql').read_text(encoding='utf-8')).make_module()
        env.globals.update(
            config=lambda **kwargs: '',
            source=lambda schema, table: 'pg_temp.yellow_taxi_raw',
            ref=lambda name: 'pg_temp.enriched',
            var=lambda name, default=None: {'cbd_schema_start_month': '2025-01'}.get(name, default),
            fingerprint_field=macros.fingerprint_field,
            raw_source_value=macros.raw_source_value,
        )
        model = env.from_string(
            (dbt / 'models/intermediate/int_taxi_trip_fingerprinted.sql').read_text(encoding='utf-8')
        ).render()
        sql = r"""
BEGIN;
CREATE TEMP TABLE yellow_taxi_raw(source_record_key text, raw_payload jsonb, schema_version text);
CREATE TEMP TABLE enriched(
  source_record_key text, schema_fingerprint text, source_year_month text,
  vendor_id smallint, pickup_datetime timestamp, dropoff_datetime timestamp,
  pu_location_id smallint, do_location_id smallint, passenger_count smallint,
  trip_distance numeric(10,3), reported_total_amount numeric(12,2)
);
INSERT INTO yellow_taxi_raw(source_record_key, raw_payload) VALUES
('k24', '{"VendorID":"1","tpep_pickup_datetime":"2024-05-01 01:02:03","tpep_dropoff_datetime":"2024-05-01 01:12:03","passenger_count":"1","trip_distance":"1.25","RatecodeID":"1","store_and_fwd_flag":"N","PULocationID":"132","DOLocationID":"138","payment_type":"1","fare_amount":"8.0","extra":"0.5","mta_tax":"0.5","tip_amount":"1.5","tolls_amount":"0","improvement_surcharge":"0.3","total_amount":"10.8","congestion_surcharge":"2.5","Airport_fee":"0"}'),
('k25', '{"VendorID":"1","tpep_pickup_datetime":"2025-05-01 01:02:03","tpep_dropoff_datetime":"2025-05-01 01:12:03","passenger_count":"1","trip_distance":"1.25","RatecodeID":"1","store_and_fwd_flag":"N","PULocationID":"132","DOLocationID":"138","payment_type":"1","fare_amount":"8.0","extra":"0.5","mta_tax":"0.5","tip_amount":"1.5","tolls_amount":"0","improvement_surcharge":"0.3","total_amount":"10.8","congestion_surcharge":"2.5","Airport_fee":"0","cbd_congestion_fee":"2.75"}');
INSERT INTO enriched VALUES
('k24', repeat('1',64), '2024-05', 1, '2024-05-01 01:02:03', '2024-05-01 01:12:03', 132, 138, 1, 1.250, 10.80),
('k25', repeat('2',64), '2025-05', 1, '2025-05-01 01:02:03', '2025-05-01 01:12:03', 132, 138, 1, 1.250, 10.80);
CREATE TEMP VIEW actual AS
__MODEL__;
DO $$ BEGIN
  IF (SELECT trim(raw_exact_fingerprint) FROM actual WHERE source_record_key='k24') <> '85578a876b06f4f020df08f495592a737c6c3c47ef6859a29bba822747e5642d'
     OR (SELECT trim(raw_exact_fingerprint) FROM actual WHERE source_record_key='k25') <> '6cc97b0cd280e63eb18fe0002ef5ba2f0e3bcf8011ad09b236c4b215d15b877d'
     OR (SELECT trim(candidate_trip_fingerprint) FROM actual WHERE source_record_key='k24') <> '0f372fc0637a43a1fe063164b872e4b87e85c03c7e159cd475fc6ec138cf66ab'
     OR (SELECT trim(candidate_trip_fingerprint) FROM actual WHERE source_record_key='k25') <> '91c6e9257136056464477c2718f2ff72c487fd4586271dc9ee9093665a96f761'
  THEN RAISE EXCEPTION 'golden fingerprint mismatch'; END IF;
END $$;
ROLLBACK;
""".replace('__MODEL__', model)
        result = subprocess.run(
            ['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
            cwd=root, input=sql, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
