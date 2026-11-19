"""FIN-04 fixture using the duplicate-ranking CTE from the actual model."""
from pathlib import Path
import subprocess
import unittest

import jinja2


class DuplicateSurvivorFixtureSQL(unittest.TestCase):
    def test_lowest_source_row_is_survivor_within_file(self):
        root = Path(__file__).resolve().parents[1]
        text = (root / 'dbt_project/models/intermediate/int_taxi_trip_classified.sql').read_text(encoding='utf-8')
        ranking = text.split('), current_cross_file_raw as (', 1)[0] + ')\nselect * from duplicate_ranked'
        env = jinja2.Environment()
        env.globals.update(
            ref=lambda name: 'pg_temp.fingerprinted',
            source=lambda schema, table: 'pg_temp.' + table,
            var=lambda name, default=None: default,
        )
        ranking = env.from_string(ranking).render()
        sql = r"""
BEGIN;
CREATE TEMP TABLE fingerprinted(
 source_record_key text, source_file_sha256 text, raw_exact_fingerprint text,
 source_row_number bigint
);
INSERT INTO fingerprinted VALUES
 ('row9','file-a','same',9),('row2','file-a','same',2),('row5','file-a','same',5),
 ('other','file-b','same',1);
CREATE TEMP VIEW ranked AS
__RANKING__;
DO $$ BEGIN
 IF (SELECT duplicate_survivor_source_record_key FROM ranked WHERE source_record_key='row9') <> 'row2'
    OR (SELECT duplicate_occurrence_number FROM ranked WHERE source_record_key='row2') <> 1
    OR (SELECT duplicate_occurrence_number FROM ranked WHERE source_record_key='row9') <> 3
    OR (SELECT duplicate_survivor_source_record_key FROM ranked WHERE source_record_key='other') <> 'other'
 THEN RAISE EXCEPTION 'duplicate survivor mismatch'; END IF;
END $$;
ROLLBACK;
""".replace('__RANKING__', ranking)
        result = subprocess.run(
            ['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
            cwd=root, input=sql, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
