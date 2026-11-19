"""Run the storage controller with the checked-in samples against a dedicated QA database."""
import csv
from hashlib import file_digest
import os
from pathlib import Path
import sys
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'deployment' / 'scripts'))
import run_24_month_backfill as runner

QA_DATABASE = 'nyc_taxi_qa_fixture_20261119'


def configure_fixture(database=QA_DATABASE, replacement_rows=128):
    if os.environ.get('POSTGRES_DB') != database:
        raise RuntimeError('Explicitly set POSTGRES_DB to ' + database)
    if '--apply-migration' in sys.argv or '--migration-only' in sys.argv:
        raise RuntimeError('Provision the dedicated fixture database separately')
    runner.load_dotenv()
    if runner.execute('SELECT current_database() AS db', fetch=True)[0]['db'] != database:
        raise RuntimeError('Unexpected database')

    qa_root = ROOT / 'evidence' / 'run_logs' / 'storage_qa'
    qa_root.mkdir(parents=True, exist_ok=True)
    rows = []
    with runner.CATALOG.open(newline='', encoding='utf-8') as catalog:
        reader = csv.DictReader(catalog)
        for row in reader:
            if row['source_year_month'] not in ('2024-05', '2025-05'):
                continue
            source = ROOT / 'tests' / 'fixtures' / 'valid' / row['file_name'].replace('.parquet', '_sample.parquet')
            if (os.environ.get('NYC_STORAGE_QA_REPLACEMENT') == '1'
                    and row['source_year_month'] == '2024-05'):
                variant_name = ('replacement_2024_05.parquet' if replacement_rows == 128
                                else 'replacement_recovery_2024_05.parquet')
                variant = qa_root / variant_name
                if not variant.exists():
                    sample = pq.read_table(source).slice(0, replacement_rows)
                    pq.write_table(pa.concat_tables([sample, sample.slice(0, 1)]), variant)
                source = variant
            with source.open('rb') as handle:
                sha256 = file_digest(handle, 'sha256').hexdigest()
            rows.append({
                **row, 'sha256': sha256, 'content_length': source.stat().st_size,
                'local_path': source.relative_to(ROOT).as_posix(),
            })
    if len(rows) != 2:
        raise RuntimeError('Expected two approved sample months')
    runner.CATALOG = qa_root / ('fixture_catalog.csv' if database == QA_DATABASE else 'fixture_recovery_catalog.csv')
    runner.DATA_ROOT = ROOT
    runner.WORK_ROOT = qa_root / ('fixture_work' if database == QA_DATABASE else 'fixture_recovery_work')
    with runner.CATALOG.open('w', newline='', encoding='utf-8') as catalog:
        writer = csv.DictWriter(catalog, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    configure_fixture()
    if os.environ.get('NYC_STORAGE_QA_FAIL_CLEANUP') == '1':
        with patch.object(runner.shutil, 'rmtree', side_effect=OSError('Injected work-file cleanup failure')):
            return runner.main()
    return runner.main()


if __name__ == '__main__':
    sys.exit(main())
