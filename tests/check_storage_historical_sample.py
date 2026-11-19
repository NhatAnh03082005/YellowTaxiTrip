"""Compare a bounded slice of the actual immutable Parquet with the restored registry."""
from contextlib import closing
from hashlib import file_digest, sha256
import os
from pathlib import Path
import subprocess
import sys

from psycopg2 import sql
from psycopg2.extras import execute_values
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'deployment' / 'scripts'))
import run_24_month_backfill as runner

RESTORE_DB = 'nyc_taxi_qa_restore_20261119'
RENDER = """
from pathlib import Path
import jinja2
root = Path('dbt_project')
env = jinja2.Environment(undefined=jinja2.StrictUndefined)
macros = env.from_string((root / 'macros/fingerprint_fields.sql').read_text()).make_module()
env.globals.update(source=lambda schema, table: 'pg_temp.' + table,
                   ref=lambda name: 'pg_temp.enriched',
                   var=lambda name: '2025-01',
                   fingerprint_field=macros.fingerprint_field,
                   raw_source_value=macros.raw_source_value)
path = root / 'models/intermediate/int_taxi_trip_fingerprinted.sql'
print(env.from_string(path.read_text()).render())
"""


def main():
    if os.environ.get('POSTGRES_DB') != RESTORE_DB:
        raise RuntimeError('Explicit restored QA database required')
    runner.load_dotenv()
    with closing(runner.connection()) as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        if cur.fetchone()[0] != RESTORE_DB:
            raise RuntimeError('Unexpected database')
        cur.execute("""
            SELECT v.file_version_id, v.source_file, trim(v.source_file_sha256),
                   trim(v.schema_fingerprint), v.schema_version
            FROM control.source_file_version v
            WHERE v.source_year_month = '2024-05'
              AND EXISTS (SELECT 1 FROM audit.trip_fingerprint_history h
                          WHERE h.file_version_id = v.file_version_id)
        """)
        [(version, file_name, checksum, schema_fingerprint, schema_version)] = cur.fetchall()
        source = ROOT / 'data/yellow_taxi/by_year_month/2024-05' / file_name
        with source.open('rb') as handle:
            if file_digest(handle, 'sha256').hexdigest() != checksum:
                raise RuntimeError('Historical Parquet checksum mismatch')
        parquet = pq.ParquetFile(source)
        group_count = parquet.metadata.num_row_groups
        groups = sorted({0, (group_count - 1) // 3, 2 * (group_count - 1) // 3, group_count - 1})
        columns = parquet.schema_arrow.names
        cur.execute("""
            CREATE TEMP TABLE yellow_taxi_raw AS
            SELECT * FROM stage.yellow_taxi_raw WITH NO DATA
        """)
        metadata = ['source_record_key', 'source_row_number', 'source_year_month',
                    'schema_fingerprint', 'schema_version']
        insert = sql.SQL('INSERT INTO pg_temp.yellow_taxi_raw ({}) VALUES %s').format(
            sql.SQL(', ').join(map(sql.Identifier, columns + metadata)))
        rows = []
        offset = 0
        for group in range(group_count):
            if group in groups:
                sample = next(parquet.iter_batches(batch_size=512, row_groups=[group]))
                for number, row in enumerate(sample.to_pylist(), offset + 1):
                    key = sha256(f'{checksum}:{number}'.encode()).hexdigest()
                    rows.append(tuple(row[column] for column in columns)
                                + (key, number, '2024-05', schema_fingerprint, schema_version))
            offset += parquet.metadata.row_group(group).num_rows
        cur.execute("SET LOCAL statement_timeout = '20s'")
        cur.execute("SET LOCAL temp_file_limit = '64MB'")
        execute_values(cur, insert, rows)
        cur.execute("""
            CREATE TEMP VIEW enriched AS
            SELECT source_record_key, source_row_number, source_year_month, schema_fingerprint,
                "VendorID"::smallint AS vendor_id,
                tpep_pickup_datetime AS pickup_datetime, tpep_dropoff_datetime AS dropoff_datetime,
                "PULocationID"::smallint AS pu_location_id, "DOLocationID"::smallint AS do_location_id,
                passenger_count::smallint, trip_distance::numeric(10,3),
                total_amount::numeric(12,2) AS reported_total_amount
            FROM pg_temp.yellow_taxi_raw
        """)
        renderer = ROOT / '.venv-dbt' / 'Scripts' / 'python.exe'
        rendered = subprocess.check_output([str(renderer), '-c', RENDER], cwd=ROOT, text=True)
        cur.execute('CREATE TEMP VIEW recomputed_fingerprints AS ' + rendered)
        cur.execute("""
            SELECT count(*) AS sampled,
                   count(h.source_record_key) AS exact_lineage,
                   count(*) FILTER (WHERE decode(trim(q.raw_exact_fingerprint), 'hex')
                                        = h.raw_exact_fingerprint) AS raw_match,
                   count(*) FILTER (WHERE decode(trim(q.candidate_trip_fingerprint), 'hex')
                                        = h.candidate_trip_fingerprint) AS candidate_match
            FROM pg_temp.recomputed_fingerprints q
            LEFT JOIN audit.trip_fingerprint_history h
              ON h.source_record_key = decode(trim(q.source_record_key), 'hex')
             AND h.source_row_number = q.source_row_number
             AND h.source_file_sha256 = decode(%s, 'hex') AND h.file_version_id = %s
        """, (checksum, version))
        counts = cur.fetchone()
        print('historical version, footer, selected/total row groups, sampled, lineage, raw match, candidate match:',
              version, parquet.metadata.num_rows, f'{len(groups)}/{group_count}', *counts)
        if not rows or counts != (len(rows),) * 4:
            raise RuntimeError('Historical sampled fingerprints or lineage differ')


if __name__ == '__main__':
    main()
