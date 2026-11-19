"""FIN-02/03 reconciliation fixture against the actual dbt model SQL."""
from pathlib import Path
import subprocess
import unittest

import jinja2


class ReconciliationFixtureSQL(unittest.TestCase):
    def test_two_versions_balance_all_three_persistent_branches(self):
        root = Path(__file__).resolve().parents[1]
        model_path = root / 'dbt_project/models/quality/file_reconciliation.sql'
        env = jinja2.Environment()
        env.globals.update(
            config=lambda **kwargs: '',
            source=lambda schema, table: 'pg_temp.' + table,
            ref=lambda name: 'pg_temp.' + name,
        )
        model = env.from_string(model_path.read_text(encoding='utf-8')).render()
        sql = r"""
BEGIN;
CREATE TEMP TABLE source_file_version(file_version_id bigint, source_file text, source_file_sha256 text);
CREATE TEMP TABLE yellow_taxi_raw(file_version_id bigint, batch_id bigint, source_file text, source_file_sha256 text, total_amount numeric);
CREATE TEMP TABLE yellow_taxi_canonical(LIKE yellow_taxi_raw);
CREATE TEMP TABLE technical_quarantine(file_version_id bigint, source_file text, source_file_sha256 text, raw_payload jsonb);
CREATE TEMP TABLE fact_taxi_trip(file_version_id bigint, total_amount numeric);
CREATE TEMP TABLE business_quarantine(LIKE fact_taxi_trip);
CREATE TEMP TABLE duplicate_quarantine(LIKE fact_taxi_trip);
INSERT INTO source_file_version VALUES (1,'v1.parquet','sha1'),(2,'v2.parquet','sha2');
INSERT INTO yellow_taxi_raw VALUES
 (1,101,'v1.parquet','sha1',10),(1,101,'v1.parquet','sha1',20),(1,101,'v1.parquet','sha1',null),
 (2,102,'v2.parquet','sha2',5),(2,102,'v2.parquet','sha2',6),(2,102,'v2.parquet','sha2',7);
INSERT INTO yellow_taxi_canonical VALUES
 (1,101,'v1.parquet','sha1',10),(1,101,'v1.parquet','sha1',20),
 (2,102,'v2.parquet','sha2',5),(2,102,'v2.parquet','sha2',6),(2,102,'v2.parquet','sha2',7);
INSERT INTO technical_quarantine VALUES (1,'v1.parquet','sha1','{"total_amount":"bad"}');
INSERT INTO fact_taxi_trip VALUES (1,10),(2,5);
INSERT INTO business_quarantine VALUES (1,20),(2,6);
INSERT INTO duplicate_quarantine VALUES (2,7);
CREATE TEMP VIEW actual AS
__MODEL__;
DO $$ BEGIN
  IF (SELECT count(*) FROM actual) <> 2
     OR EXISTS (SELECT 1 FROM actual WHERE source_row_delta <> 0 OR trusted_row_delta <> 0
       OR source_amount_eligible_row_delta <> 0 OR trusted_amount_eligible_row_delta <> 0
       OR source_amount_delta <> 0 OR trusted_amount_delta <> 0)
     OR (SELECT duplicate_rows FROM actual WHERE file_version_id=2) <> 1
     OR (SELECT technical_rows FROM actual WHERE file_version_id=1) <> 1
  THEN RAISE EXCEPTION 'reconciliation fixture mismatch'; END IF;
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
