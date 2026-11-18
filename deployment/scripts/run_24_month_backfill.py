"""Run the storage-optimized Yellow Taxi pipeline one immutable file at a time.

This is the master controller missing from the Phase 2 Mage-only ingestion DAG.
It reuses the Mage block functions, runs dbt DDS/DQ/tests/OLAP, persists
reconciliation, commits the watermark, and only then cleans transient Staging.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import psycopg2
from psycopg2.extras import RealDictCursor


ROOT = Path(__file__).resolve().parents[2]
MAGE_ROOT = ROOT / 'mage_project'
DBT_ROOT = ROOT / 'dbt_project'
DATA_ROOT = ROOT / 'data'
WORK_ROOT = DATA_ROOT / 'work' / 'task_4_4'
CATALOG = DATA_ROOT / 'catalog' / 'yellow_taxi_2024_2025.csv'
SCHEMA_REGISTRY = DATA_ROOT / 'catalog' / 'yellow_taxi_schema_registry.csv'


def load_dotenv() -> None:
    for line in (ROOT / '.env').read_text(encoding='utf-8').splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#') or '=' not in stripped:
            continue
        key, value = stripped.split('=', 1)
        os.environ.setdefault(key, value)
    os.environ['POSTGRES_HOST'] = 'localhost'
    os.environ['POSTGRES_PORT'] = os.environ.get('POSTGRES_HOST_PORT', '5433')
    os.environ['DBT_HOST'] = 'localhost'
    os.environ['DBT_PORT'] = os.environ['POSTGRES_PORT']
    os.environ['DBT_PROFILES_DIR'] = str(ROOT / '.dbt')


def connection():
    return psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=int(os.environ['POSTGRES_PORT']),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )


def execute(sql: str, params=None, fetch=False):
    with connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params or {})
            if fetch:
                return cursor.fetchall()
    return None


def docker_psql(sql_path: Path) -> None:
    text = '\n'.join(
        line for line in sql_path.read_text(encoding='utf-8').splitlines()
        if not line.lstrip().startswith('\\')
    ) + '\n'
    command = [
        'docker', 'compose', 'exec', '-T', 'postgres', 'psql',
        '-v', 'ON_ERROR_STOP=1', '-U', os.environ['POSTGRES_USER'],
        '-d', os.environ['POSTGRES_DB'],
    ]
    print('Applying {}'.format(sql_path.relative_to(ROOT)), flush=True)
    subprocess.run(command, cwd=ROOT, input=text, text=True, check=True)


def apply_migration() -> None:
    docker_psql(ROOT / 'sql' / 'migrations' / '005_storage_optimization_v2.sql')
    docker_psql(ROOT / 'sql' / 'control' / '003_control_functions.sql')
    docker_psql(ROOT / 'sql' / 'control' / '002_control_indexes.sql')
    docker_psql(ROOT / 'sql' / 'stage' / '002_stage_indexes.sql')


def dbt_executable() -> str:
    candidate = ROOT / '.venv-dbt' / 'Scripts' / 'dbt.exe'
    return str(candidate) if candidate.is_file() else 'dbt'


def run_dbt(arguments: list[str], batch_id: int) -> None:
    variables = json.dumps({
        'load_batch_id': batch_id,
        'fact_lifecycle_mode': 'archive',
    })
    command = [
        dbt_executable(), *arguments, '--project-dir', str(DBT_ROOT),
        '--profiles-dir', str(ROOT / '.dbt'), '--vars', variables,
    ]
    print('Running: dbt {}'.format(' '.join(arguments)), flush=True)
    subprocess.run(command, cwd=ROOT, env=os.environ.copy(), check=True)


def control_step(batch_id: int, step: str, action: str, message=None) -> None:
    if action == 'start':
        execute('SELECT control.start_step(%(batch)s, %(step)s);', {
            'batch': batch_id, 'step': step,
        })
    else:
        status = 'SUCCESS' if action == 'success' else 'FAILED'
        execute(
            """
            SELECT control.finish_step(
                %(batch)s, %(step)s, %(status)s,
                CASE WHEN %(status)s = 'SUCCESS' THEN 0 ELSE 1 END,
                '{}'::jsonb, %(message)s
            );
            """,
            {'batch': batch_id, 'step': step, 'status': status,
             'message': None if message is None else str(message)[:2000]},
        )


def mark_skipped_downstream_success(batch_id: int) -> None:
    execute("SELECT control.transition_batch(%(batch)s, 'DBT_RUNNING');", {'batch': batch_id})
    for step in ('DBT_BUILD_DDS', 'DBT_TEST'):
        control_step(batch_id, step, 'start')
        control_step(batch_id, step, 'success')
    execute("SELECT control.transition_batch(%(batch)s, 'RECONCILING');", {'batch': batch_id})
    for step in ('RECONCILIATION', 'DBT_BUILD_OLAP'):
        control_step(batch_id, step, 'start')
        control_step(batch_id, step, 'success')
    execute('SELECT control.commit_batch_success(%(batch)s);', {'batch': batch_id})


def persist_reconciliation(batch_id: int) -> dict:
    rows = execute(
        'SELECT * FROM dq.file_reconciliation WHERE batch_id = %(batch)s;',
        {'batch': batch_id}, fetch=True,
    )
    if not rows:
        raise RuntimeError('No reconciliation rows produced for batch {}'.format(batch_id))
    for row in rows:
        if row['source_row_delta'] != 0 or row['trusted_row_delta'] != 0:
            raise RuntimeError('Row reconciliation failed for file version {}'.format(
                row['file_version_id']
            ))
        if (row['source_amount_eligible_row_delta'] != 0
                or row['trusted_amount_eligible_row_delta'] != 0
                or abs(row['source_amount_delta']) > 0.01
                or abs(row['trusted_amount_delta']) > 0.01):
            raise RuntimeError('Amount reconciliation failed for file version {}'.format(
                row['file_version_id']
            ))
        execute(
            """
            INSERT INTO control.file_reconciliation_history (
                file_version_id, successful_batch_id,
                source_rows, canonical_rows, technical_rows,
                fact_rows, business_rows, duplicate_rows,
                source_amount_eligible_rows, canonical_amount_eligible_rows,
                technical_amount_eligible_rows, fact_amount_eligible_rows,
                business_amount_eligible_rows, duplicate_amount_eligible_rows,
                source_amount, canonical_amount, technical_amount,
                fact_amount, business_amount, duplicate_amount,
                source_row_delta, trusted_row_delta,
                source_amount_delta, trusted_amount_delta, recorded_at
            ) VALUES (
                %(file_version_id)s, %(batch_id)s,
                %(raw_rows)s, %(canonical_rows)s, %(technical_rows)s,
                %(fact_rows)s, %(business_rows)s, %(duplicate_rows)s,
                %(raw_amount_eligible_rows)s, %(canonical_amount_eligible_rows)s,
                %(technical_amount_eligible_rows)s, %(fact_amount_eligible_rows)s,
                %(business_amount_eligible_rows)s, %(duplicate_amount_eligible_rows)s,
                %(raw_amount)s, %(canonical_amount)s, %(technical_amount)s,
                %(fact_amount)s, %(business_amount)s, %(duplicate_amount)s,
                %(source_row_delta)s, %(trusted_row_delta)s,
                %(source_amount_delta)s, %(trusted_amount_delta)s, clock_timestamp()
            )
            ON CONFLICT (file_version_id) DO UPDATE SET
                successful_batch_id = EXCLUDED.successful_batch_id,
                source_rows = EXCLUDED.source_rows,
                canonical_rows = EXCLUDED.canonical_rows,
                technical_rows = EXCLUDED.technical_rows,
                fact_rows = EXCLUDED.fact_rows,
                business_rows = EXCLUDED.business_rows,
                duplicate_rows = EXCLUDED.duplicate_rows,
                source_amount_eligible_rows = EXCLUDED.source_amount_eligible_rows,
                canonical_amount_eligible_rows = EXCLUDED.canonical_amount_eligible_rows,
                technical_amount_eligible_rows = EXCLUDED.technical_amount_eligible_rows,
                fact_amount_eligible_rows = EXCLUDED.fact_amount_eligible_rows,
                business_amount_eligible_rows = EXCLUDED.business_amount_eligible_rows,
                duplicate_amount_eligible_rows = EXCLUDED.duplicate_amount_eligible_rows,
                source_amount = EXCLUDED.source_amount,
                canonical_amount = EXCLUDED.canonical_amount,
                technical_amount = EXCLUDED.technical_amount,
                fact_amount = EXCLUDED.fact_amount,
                business_amount = EXCLUDED.business_amount,
                duplicate_amount = EXCLUDED.duplicate_amount,
                source_row_delta = EXCLUDED.source_row_delta,
                trusted_row_delta = EXCLUDED.trusted_row_delta,
                source_amount_delta = EXCLUDED.source_amount_delta,
                trusted_amount_delta = EXCLUDED.trusted_amount_delta,
                recorded_at = EXCLUDED.recorded_at;
            """,
            {**row, 'batch_id': batch_id},
        )
    totals = {
        key: sum(int(row[key]) for row in rows)
        for key in ('raw_rows', 'canonical_rows', 'technical_rows',
                    'fact_rows', 'business_rows', 'duplicate_rows',
                    'raw_amount_eligible_rows')
    }
    execute(
        """
        SELECT control.update_batch_counters(
            %(batch)s, %(raw)s, %(canonical)s, %(technical)s,
            %(business)s, %(duplicate)s, %(fact)s, %(amount_eligible)s
        );
        """,
        {'batch': batch_id, 'raw': totals['raw_rows'],
         'canonical': totals['canonical_rows'], 'technical': totals['technical_rows'],
         'business': totals['business_rows'], 'duplicate': totals['duplicate_rows'],
         'fact': totals['fact_rows'],
         'amount_eligible': totals['raw_amount_eligible_rows']},
    )
    return {'rows': rows, 'totals': totals}


def run_downstream(batch_id: int) -> dict:
    state = execute(
        'SELECT status FROM control.etl_batch WHERE batch_id = %(batch)s;',
        {'batch': batch_id}, fetch=True,
    )
    if not state:
        raise RuntimeError('Batch {} does not exist'.format(batch_id))
    if state[0]['status'] == 'STAGED':
        execute("SELECT control.transition_batch(%(batch)s, 'DBT_RUNNING');", {'batch': batch_id})
    elif state[0]['status'] == 'DBT_RUNNING':
        execute(
            """
            UPDATE control.etl_step_run
            SET status = 'PENDING', started_at = NULL, ended_at = NULL,
                exit_code = NULL, details = '{}'::jsonb, error_message = NULL
            WHERE batch_id = %(batch)s
              AND step_name IN ('DBT_BUILD_DDS', 'DBT_TEST', 'RECONCILIATION', 'DBT_BUILD_OLAP')
              AND status <> 'SUCCESS';
            """,
            {'batch': batch_id},
        )
    else:
        raise RuntimeError(
            'Batch {} cannot resume downstream from {}'.format(
                batch_id, state[0]['status']
            )
        )
    try:
        control_step(batch_id, 'DBT_BUILD_DDS', 'start')
        run_dbt(['seed'], batch_id)
        run_dbt(['run', '--exclude', 'tag:olap'], batch_id)
        control_step(batch_id, 'DBT_BUILD_DDS', 'success')

        control_step(batch_id, 'DBT_TEST', 'start')
        run_dbt(['test'], batch_id)
        control_step(batch_id, 'DBT_TEST', 'success')

        execute("SELECT control.transition_batch(%(batch)s, 'RECONCILING');", {'batch': batch_id})
        control_step(batch_id, 'RECONCILIATION', 'start')
        result = persist_reconciliation(batch_id)
        control_step(batch_id, 'RECONCILIATION', 'success')

        control_step(batch_id, 'DBT_BUILD_OLAP', 'start')
        run_dbt(['run', '--select', 'tag:olap'], batch_id)
        control_step(batch_id, 'DBT_BUILD_OLAP', 'success')

        execute('SELECT control.commit_batch_success(%(batch)s);', {'batch': batch_id})
        return result
    except Exception as exception:
        try:
            execute(
                "SELECT control.fail_batch(%(batch)s, 'DOWNSTREAM_FAILED', %(message)s);",
                {'batch': batch_id, 'message': str(exception)[:2000]},
            )
        finally:
            raise


def clean_transient_storage(batch_id: int) -> None:
    rows = execute(
        'SELECT status FROM control.etl_batch WHERE batch_id = %(batch)s;',
        {'batch': batch_id}, fetch=True,
    )
    if not rows or rows[0]['status'] != 'SUCCESS':
        raise RuntimeError('Refusing cleanup before batch SUCCESS')
    execute(
        """
        TRUNCATE TABLE stage.yellow_taxi_raw, stage.yellow_taxi_canonical
            RESTART IDENTITY;
        SELECT control.log_event(
            'pl_taxi_master', 'INFO', 'Transient Staging cleaned after SUCCESS',
            %(batch)s, p_event_code => 'STAGING_CLEANED'
        );
        """,
        {'batch': batch_id},
    )
    batch_root = (WORK_ROOT / str(batch_id)).resolve()
    batch_root.relative_to(WORK_ROOT.resolve())
    if batch_root.is_dir():
        shutil.rmtree(batch_root)


def run_ingestion(month: str) -> dict:
    if str(MAGE_ROOT) not in sys.path:
        sys.path.insert(0, str(MAGE_ROOT))
    from data_loaders.get_extraction_window import get_extraction_window
    from transformers.discover_source_files import discover_source_files
    from transformers.verify_checksum import verify_checksum
    from transformers.filter_incremental import filter_incremental
    from transformers.load_source_files import load_source_files
    from transformers.canonicalize_schema import canonicalize_schema
    from transformers.standardize_and_validate import standardize_and_validate
    from data_exporters.write_technical_quarantine import write_technical_quarantine
    from data_exporters.load_stage import load_stage
    from transformers.verify_stage import verify_stage

    context = get_extraction_window(run_type='BACKFILL', triggered_by='backfill_controller')
    context = discover_source_files(
        context, catalog_path=str(CATALOG), source_year_month_from=month,
        source_year_month_to=month,
    )
    context = verify_checksum(
        context, schema_registry_path=str(SCHEMA_REGISTRY), data_root=str(DATA_ROOT),
    )
    context = filter_incremental(context)
    process_count = context['process_file_count']
    context = load_source_files(
        context, work_root=str(WORK_ROOT), source_batch_size=50000,
    )
    context = canonicalize_schema(
        context, work_root=str(WORK_ROOT), canonical_batch_size=50000,
    )
    context = standardize_and_validate(context)
    context = write_technical_quarantine(context)
    context = load_stage(context)
    staged = verify_stage(context)
    return {**staged, 'process_file_count': process_count}


def month_rows(month_from: str, month_to: str) -> list[str]:
    with CATALOG.open(newline='', encoding='utf-8') as handle:
        return [
            row['source_year_month'] for row in csv.DictReader(handle)
            if month_from <= row['source_year_month'] <= month_to
        ]


def create_backfill_run(months: list[str]) -> int:
    rows = execute(
        """
        INSERT INTO control.etl_backfill_run (
            pipeline_name, source_name, source_month_from, source_month_to,
            expected_file_count
        ) VALUES (
            'pl_taxi_master', 'yellow_taxi_tripdata', %(month_from)s,
            %(month_to)s, %(count)s
        ) RETURNING backfill_run_id;
        """,
        {'month_from': months[0], 'month_to': months[-1], 'count': len(months)},
        fetch=True,
    )
    run_id = int(rows[0]['backfill_run_id'])
    for month in months:
        execute(
            """
            INSERT INTO control.etl_backfill_file (backfill_run_id, source_year_month)
            VALUES (%(run)s, %(month)s);
            """,
            {'run': run_id, 'month': month},
        )
    return run_id


def update_backfill_file(run_id: int, month: str, status: str,
                         batch_id=None, message=None) -> None:
    execute(
        """
        UPDATE control.etl_backfill_file f
        SET status = %(status)s,
            batch_id = coalesce(%(batch)s, f.batch_id),
            file_version_id = coalesce((
                SELECT max(v.file_version_id)
                FROM control.source_file_version v
                WHERE v.source_year_month = %(month)s
            ), f.file_version_id),
            completed_at = CASE WHEN %(status)s IN ('SUCCESS', 'FAILED')
                THEN clock_timestamp() ELSE NULL END,
            error_message = %(message)s
        WHERE f.backfill_run_id = %(run)s
          AND f.source_year_month = %(month)s;
        UPDATE control.etl_backfill_run r
        SET completed_file_count = (
            SELECT count(*) FROM control.etl_backfill_file f
            WHERE f.backfill_run_id = r.backfill_run_id AND f.status = 'SUCCESS'
        )
        WHERE r.backfill_run_id = %(run)s;
        """,
        {'run': run_id, 'month': month, 'status': status, 'batch': batch_id,
         'message': None if message is None else str(message)[:2000]},
    )


def finalize_backfill(run_id: int, success: bool, message=None) -> None:
    execute(
        """
        UPDATE control.etl_backfill_run
        SET status = %(status)s, ended_at = clock_timestamp(), error_message = %(message)s
        WHERE backfill_run_id = %(run)s;
        """,
        {'run': run_id, 'status': 'SUCCESS' if success else 'FAILED',
         'message': None if message is None else str(message)[:2000]},
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-month', default='2024-01')
    parser.add_argument('--to-month', default='2025-12')
    parser.add_argument('--resume-batch', type=int)
    parser.add_argument('--apply-migration', action='store_true')
    parser.add_argument('--migration-only', action='store_true')
    parser.add_argument('--no-cleanup', action='store_true')
    args = parser.parse_args()
    load_dotenv()
    if args.apply_migration:
        apply_migration()
    if args.migration_only:
        return 0

    if args.resume_batch:
        result = run_downstream(args.resume_batch)
        if not args.no_cleanup:
            clean_transient_storage(args.resume_batch)
        print(json.dumps({'batch_id': args.resume_batch, 'result': result}, default=str))
        return 0

    months = month_rows(args.from_month, args.to_month)
    if not months:
        raise ValueError('No catalog months in requested scope')
    run_id = create_backfill_run(months)
    try:
        for position, month in enumerate(months, start=1):
            print('[{}/{}] Processing {}'.format(position, len(months), month), flush=True)
            update_backfill_file(run_id, month, 'RUNNING')
            ingestion = run_ingestion(month)
            batch_id = int(ingestion['batch_id'])
            if ingestion['process_file_count'] == 0:
                mark_skipped_downstream_success(batch_id)
            else:
                run_downstream(batch_id)
            if not args.no_cleanup:
                clean_transient_storage(batch_id)
            update_backfill_file(run_id, month, 'SUCCESS', batch_id=batch_id)
        finalize_backfill(run_id, True)
        print('Backfill run {} completed 24/24 files'.format(run_id), flush=True)
        return 0
    except Exception as exception:
        update_backfill_file(run_id, month, 'FAILED',
                             batch_id=locals().get('batch_id'), message=exception)
        finalize_backfill(run_id, False, exception)
        raise


if __name__ == '__main__':
    raise SystemExit(main())

