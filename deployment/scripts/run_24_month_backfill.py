"""Run the storage-optimized Yellow Taxi pipeline one immutable file at a time.

This is the master controller missing from the Phase 2 Mage-only ingestion DAG.
It reuses the Mage block functions, runs dbt DDS/DQ/tests/OLAP, persists
reconciliation, commits the watermark, and only then cleans transient Staging.
"""

from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
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


def check_disk_safety(expected_peak_bytes: int) -> dict:
    if expected_peak_bytes is None or expected_peak_bytes <= 0:
        raise RuntimeError('A positive --expected-peak-bytes forecast is required')
    usage = shutil.disk_usage(ROOT)
    reserved = (usage.total + 3) // 4
    projected_free = usage.free - expected_peak_bytes
    if projected_free < reserved:
        raise RuntimeError(
            'Disk safety gate failed: projected free {} bytes is below 25% reserve {} bytes'.format(
                projected_free, reserved
            )
        )
    return {
        'total_bytes': usage.total, 'free_bytes': usage.free,
        'expected_peak_bytes': expected_peak_bytes,
        'projected_free_bytes': projected_free, 'reserved_bytes': reserved,
    }


def validate_v2_approval(path_value: str) -> Path:
    if os.environ.get('ALLOW_24_MONTH_BACKFILL') != '1':
        raise RuntimeError('Full backfill disabled: ALLOW_24_MONTH_BACKFILL=1 is required')
    path = Path(path_value or '').resolve()
    try:
        path.relative_to(ROOT.resolve())
    except ValueError as exception:
        raise RuntimeError('V2 approval evidence must be inside the project') from exception
    if not path.is_file():
        raise RuntimeError('V2 approval evidence file does not exist inside the project')
    text = path.read_text(encoding='utf-8')
    required = ('Project Lead / Integration | APPROVED', 'dbt Owner | APPROVED')
    if not all(item in text for item in required):
        raise RuntimeError('V2 approval evidence does not contain both APPROVED owners')
    return path


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
            return execute_cursor(cursor, sql, params, fetch)


def execute_cursor(cursor, sql, params=None, fetch=False):
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
    docker_psql(ROOT / 'sql' / 'migrations' / '006_backfill_recovery_and_cleanup.sql')
    docker_psql(ROOT / 'sql' / 'migrations' / '007_replacement_recovery.sql')
    docker_psql(ROOT / 'sql' / 'control' / '003_control_functions.sql')
    docker_psql(ROOT / 'sql' / 'control' / '002_control_indexes.sql')
    docker_psql(ROOT / 'sql' / 'stage' / '002_stage_indexes.sql')


def dbt_executable() -> str:
    candidate = ROOT / '.venv-dbt' / 'Scripts' / 'dbt.exe'
    return str(candidate) if candidate.is_file() else 'dbt'


def run_dbt(arguments: list[str], batch_id: int, extra_vars=None) -> None:
    variables = json.dumps({
        'load_batch_id': batch_id,
        'fact_lifecycle_mode': 'archive',
        **(extra_vars or {}),
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
    raise RuntimeError(
        'Batch {} contains no processable file; refusing to fabricate downstream success'.format(batch_id)
    )


def validate_reconciliation(batch_id: int, query=None) -> list:
    query = execute if query is None else query
    rows = query(
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
    return rows


def persist_reconciliation(batch_id: int, query) -> dict:
    rows = validate_reconciliation(batch_id, query)
    for row in rows:
        query(
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
    query(
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
        '''SELECT b.status, r.batch_id IS NOT NULL AS replacement
           FROM control.etl_batch b LEFT JOIN control.etl_replacement r USING (batch_id)
           WHERE b.batch_id = %(batch)s;''',
        {'batch': batch_id}, fetch=True,
    )
    if not state:
        raise RuntimeError('Batch {} does not exist'.format(batch_id))
    replacement = state[0].get('replacement', False)
    if replacement and state[0]['status'] == 'SUCCESS':
        return {'batch_id': batch_id, 'already_committed': True}
    if state[0]['status'] not in ('FAILED', 'STAGED', 'DBT_RUNNING', 'RECONCILING'):
        raise RuntimeError(
            'Batch {} cannot resume downstream from {}'.format(
                batch_id, state[0]['status']
            )
        )
    execute('SELECT control.resume_failed_downstream(%(batch)s);', {'batch': batch_id})
    def dbt(args):
        if replacement:
            run_dbt(args, batch_id, {'apply_approved_fact_replacement': True})
        else:
            run_dbt(args, batch_id)

    try:
        control_step(batch_id, 'DBT_BUILD_DDS', 'start')
        dbt(['seed'])
        dbt(['run', '--exclude', 'tag:olap'])
        control_step(batch_id, 'DBT_BUILD_DDS', 'success')

        control_step(batch_id, 'DBT_TEST', 'start')
        dbt(['test'])
        control_step(batch_id, 'DBT_TEST', 'success')

        execute("SELECT control.transition_batch(%(batch)s, 'RECONCILING');", {'batch': batch_id})
        control_step(batch_id, 'RECONCILIATION', 'start')
        validate_reconciliation(batch_id)
        control_step(batch_id, 'RECONCILIATION', 'success')

        control_step(batch_id, 'DBT_BUILD_OLAP', 'start')
        dbt(['run', '--select', 'tag:olap'])
        control_step(batch_id, 'DBT_BUILD_OLAP', 'success')

        with connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                def query(sql, params=None, fetch=False):
                    return execute_cursor(cursor, sql, params, fetch)

                result = persist_reconciliation(batch_id, query)
                query('SELECT control.commit_batch_success(%(batch)s);', {'batch': batch_id})
                query("""
                    UPDATE control.etl_backfill_file SET status = 'SUCCESS',
                        completed_at = clock_timestamp(), error_message = NULL
                    WHERE batch_id = %(batch)s;
                    UPDATE control.etl_backfill_run r
                    SET completed_file_count = (
                        SELECT count(*) FROM control.etl_backfill_file f
                        WHERE f.backfill_run_id = r.backfill_run_id AND f.status = 'SUCCESS'
                    )
                    WHERE EXISTS (SELECT 1 FROM control.etl_backfill_file f
                        WHERE f.backfill_run_id = r.backfill_run_id AND f.batch_id = %(batch)s);
                """, {'batch': batch_id})
        return result
    except Exception as exception:
        try:
            execute(
                """SELECT control.fail_batch(batch_id, 'DOWNSTREAM_FAILED', %(message)s)
                   FROM control.etl_batch WHERE batch_id = %(batch)s AND status <> 'SUCCESS';""",
                {'batch': batch_id, 'message': str(exception)[:2000]},
            )
        finally:
            raise


def clean_transient_storage(batch_id: int) -> None:
    """Clean the single-writer staging scope, with a durable retry checkpoint."""
    if os.environ.get('ALLOW_STAGING_CLEANUP') != '1':
        raise RuntimeError('cleanup disabled: set ALLOW_STAGING_CLEANUP=1 only after gate approval')
    try:
        with connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                execute_cursor(cursor, "SELECT pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'))")
                rows = execute_cursor(cursor, """
                    SELECT b.status, f.cleanup_status
                    FROM control.etl_batch b
                    JOIN control.etl_backfill_file f ON f.batch_id = b.batch_id
                    WHERE b.batch_id = %(batch)s
                    FOR UPDATE OF b, f;
                """, {'batch': batch_id}, fetch=True)
                if not rows or rows[0]['status'] != 'SUCCESS':
                    raise RuntimeError('Refusing cleanup before batch SUCCESS')
                if rows[0]['cleanup_status'] == 'SUCCESS':
                    return
                execute_cursor(cursor, """
                    LOCK TABLE stage.yellow_taxi_raw, stage.yellow_taxi_canonical
                    IN ACCESS EXCLUSIVE MODE;
                """)
                safety = execute_cursor(cursor, """
                    SELECT
                      (SELECT count(*) FROM stage.yellow_taxi_raw WHERE batch_id <> %(batch)s)
                      + (SELECT count(*) FROM stage.yellow_taxi_canonical WHERE batch_id <> %(batch)s)
                      AS foreign_rows,
                      (SELECT count(*) FROM control.etl_batch
                       WHERE batch_id <> %(batch)s
                         AND pipeline_name = 'pl_taxi_master'
                         AND status NOT IN ('SUCCESS', 'FAILED', 'CANCELLED'))
                      AS active_batches;
                """, {'batch': batch_id}, fetch=True)[0]
                if safety['foreign_rows']:
                    raise RuntimeError('Refusing cleanup: Staging contains another batch')
                if safety['active_batches']:
                    raise RuntimeError('Refusing cleanup: another production batch is active')
                execute_cursor(cursor, """
                    UPDATE control.etl_backfill_file
                    SET cleanup_status = 'RUNNING', cleanup_error = NULL
                    WHERE batch_id = %(batch)s;
                    TRUNCATE TABLE stage.yellow_taxi_raw, stage.yellow_taxi_canonical
                        RESTART IDENTITY;
                """, {'batch': batch_id})
    except Exception as exception:
        execute("""
            UPDATE control.etl_backfill_file
            SET cleanup_status = 'FAILED', cleanup_error = %(message)s
            WHERE batch_id = %(batch)s AND cleanup_status <> 'SUCCESS'
              AND EXISTS (SELECT 1 FROM control.etl_batch b
                          WHERE b.batch_id = %(batch)s AND b.status = 'SUCCESS');
        """, {'batch': batch_id, 'message': str(exception)[:2000]})
        raise

    try:
        batch_root = WORK_ROOT.resolve() / str(batch_id)
        if batch_root.is_symlink() or batch_root.resolve() != batch_root:
            raise RuntimeError('Refusing cleanup: batch work path redirects elsewhere')
        if batch_root.is_dir():
            shutil.rmtree(batch_root)
        execute("""
            UPDATE control.etl_backfill_file
            SET cleanup_status = 'SUCCESS', cleaned_at = clock_timestamp(), cleanup_error = NULL
            WHERE batch_id = %(batch)s;
            SELECT control.log_event(
                'pl_taxi_master', 'INFO', 'Transient Staging cleaned after SUCCESS',
                %(batch)s, p_event_code => 'STAGING_CLEANED'
            );
        """, {'batch': batch_id})
    except Exception as exception:
        execute("""
            UPDATE control.etl_backfill_file
            SET cleanup_status = 'FAILED', cleanup_error = %(message)s
            WHERE batch_id = %(batch)s;
        """, {'batch': batch_id, 'message': str(exception)[:2000]})
        raise


@contextmanager
def runner_lock():
    with closing(connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(hashtext('yellow_taxi_backfill_runner'))")
            if not cursor.fetchone()[0]:
                raise RuntimeError('Another backfill runner is active')
        yield


def require_no_pending_cleanup(resuming_batch=None) -> None:
    pending = execute("""
        SELECT f.batch_id FROM control.etl_backfill_file f
        JOIN control.etl_batch b USING (batch_id)
        WHERE b.status = 'SUCCESS' AND f.cleanup_status <> 'SUCCESS'
          AND f.batch_id IS DISTINCT FROM %(resume)s
        ORDER BY f.batch_id LIMIT 1;
    """, {'resume': resuming_batch}, fetch=True)
    if pending:
        raise RuntimeError('Cleanup pending for batch {}; use --retry-cleanup'.format(pending[0]['batch_id']))


def require_no_pending_replacement(resuming_batch=None) -> None:
    pending = execute("""
        SELECT r.batch_id FROM control.etl_replacement r
        JOIN control.etl_batch b USING (batch_id)
        WHERE b.status <> 'SUCCESS' AND r.batch_id IS DISTINCT FROM %(resume)s
        ORDER BY r.batch_id LIMIT 1;
    """, {'resume': resuming_batch}, fetch=True)
    if pending:
        raise RuntimeError('Replacement pending for batch {}; resume the bound batch'.format(pending[0]['batch_id']))


def authorize_replay(context: dict, file_version_id: int,
                     requested_by: str, reason: str) -> dict:
    files = context.get('files', [])
    if len(files) != 1:
        raise RuntimeError('Replay requires exactly one verified catalog file')
    item = files[0]
    if item.get('action') != 'SKIP_ALREADY_PROCESSED':
        raise RuntimeError('Replay requires an already successful immutable file version')
    if int(item.get('file_version_id', 0)) != int(file_version_id):
        raise RuntimeError('Replay file_version_id does not match verified Parquet checksum')
    execute(
        "SELECT control.authorize_file_replay(%(file)s, %(version)s, %(actor)s, %(reason)s);",
        {'file': int(item['etl_file_id']), 'version': int(file_version_id),
         'actor': requested_by, 'reason': reason},
    )
    return {
        **context,
        'files': [{**item, 'action': 'REPROCESS_IDEMPOTENTLY'}],
        'replay_file_version_id': int(file_version_id),
    }


def require_safe_file_version(context: dict) -> None:
    for item in context['files']:
        previous = execute("""
            SELECT 1 FROM control.etl_file current_attempt
            JOIN control.etl_batch current_batch ON current_batch.batch_id = current_attempt.batch_id
            JOIN control.etl_file prior ON prior.source_file = current_attempt.source_file
            JOIN control.etl_batch prior_batch ON prior_batch.batch_id = prior.batch_id
            WHERE current_attempt.etl_file_id = %(file)s
              AND prior.file_status = 'STAGED' AND prior_batch.status = 'SUCCESS'
              AND prior_batch.pipeline_name = current_batch.pipeline_name
              AND prior_batch.source_name = current_batch.source_name
              AND prior.file_version_id <> current_attempt.file_version_id
            LIMIT 1;
        """, {'file': item['etl_file_id']}, fetch=True)
        if previous:
            message = 'Replacement blocked until prior Fact retirement and historical lineage are verified'
            execute("""
                SELECT control.fail_batch(%(batch)s, 'SOURCE_VERSION_REPLACEMENT_BLOCKED', %(message)s);
            """, {'batch': context['batch_id'], 'message': message})
            raise RuntimeError(message)


def run_ingestion(month: str, replay_file_version_id=None,
                  replay_requested_by=None, replay_reason=None) -> dict:
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
    if replay_file_version_id is not None:
        context = authorize_replay(
            context, replay_file_version_id, replay_requested_by, replay_reason,
        )
    context = filter_incremental(context)
    require_safe_file_version(context)
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
        WITH parent AS (
            INSERT INTO control.etl_backfill_run (
                pipeline_name, source_name, source_month_from, source_month_to,
                expected_file_count
            ) VALUES (
                'pl_taxi_master', 'yellow_taxi_tripdata', %(month_from)s,
                %(month_to)s, cardinality(%(months)s::text[])
            ) RETURNING backfill_run_id
        )
        INSERT INTO control.etl_backfill_file (backfill_run_id, source_year_month)
        SELECT p.backfill_run_id, m.source_year_month
        FROM parent p CROSS JOIN unnest(%(months)s::text[]) m(source_year_month)
        RETURNING backfill_run_id;
        """,
        {'month_from': months[0], 'month_to': months[-1], 'months': months},
        fetch=True,
    )
    return int(rows[0]['backfill_run_id'])


def update_backfill_file(run_id: int, month: str, status: str,
                         batch_id=None, message=None) -> None:
    execute(
        """
        UPDATE control.etl_backfill_file f
        SET status = %(status)s,
            batch_id = coalesce(%(batch)s, f.batch_id),
            file_version_id = coalesce((
                SELECT attempt.file_version_id
                FROM control.etl_file attempt
                WHERE attempt.batch_id = %(batch)s
                  AND attempt.source_year_month = %(month)s
                  AND attempt.file_version_id IS NOT NULL
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
    rows = execute(
        """
        UPDATE control.etl_backfill_run
        SET status = %(status)s, ended_at = clock_timestamp(), error_message = %(message)s
        WHERE backfill_run_id = %(run)s
          AND (%(status)s = 'FAILED' OR (
              expected_file_count = 24 AND completed_file_count = expected_file_count
              AND source_month_from = '2024-01' AND source_month_to = '2025-12'
              AND dds_validation_status = 'SUCCESS'
              AND olap_bi_validation_status = 'SUCCESS'
              AND EXISTS (
                  SELECT 1 FROM control.v_backfill_global_reconciliation g
                  WHERE g.backfill_run_id = %(run)s
                    AND g.validated_file_count = 24
                    AND g.successful_child_count = 24
                    AND g.cleanup_success_count = 24
                    AND g.required_step_failure_count = 0
                    AND g.is_balanced
                    AND g.source_rows = 89892322
              )
          ))
        RETURNING backfill_run_id;
        """,
        {'run': run_id, 'status': 'SUCCESS' if success else 'FAILED',
         'message': None if message is None else str(message)[:2000]}, fetch=True,
    )
    if not rows:
        raise RuntimeError('Backfill run {} did not satisfy finalization gates'.format(run_id))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-month', default='2024-01')
    parser.add_argument('--to-month', default='2025-12')
    recovery = parser.add_mutually_exclusive_group()
    recovery.add_argument('--resume-batch', type=int)
    recovery.add_argument('--retry-cleanup', type=int)
    recovery.add_argument('--continue-run', type=int)
    recovery.add_argument('--finalize-run', type=int)
    parser.add_argument('--apply-migration', action='store_true')
    parser.add_argument('--migration-only', action='store_true')
    parser.add_argument('--no-cleanup', action='store_true', help='Retain staging (required until cleanup gates pass)')
    parser.add_argument('--enable-cleanup', action='store_true', help='Use gated single-writer cleanup')
    parser.add_argument('--replay-file-version', type=int)
    parser.add_argument('--replay-requested-by')
    parser.add_argument('--replay-reason')
    parser.add_argument('--expected-peak-bytes', type=int)
    parser.add_argument('--v2-approval-file')
    args = parser.parse_args()
    load_dotenv()
    if args.apply_migration:
        apply_migration()
    if args.migration_only:
        return 0
    if args.finalize_run:
        finalize_backfill(args.finalize_run, True)
        print('Backfill parent {} finalized after all gates'.format(args.finalize_run), flush=True)
        return 0
    if args.retry_cleanup:
        with runner_lock():
            clean_transient_storage(args.retry_cleanup)
        return 0

    if args.no_cleanup == args.enable_cleanup:
        parser.error('choose exactly one of --no-cleanup or --enable-cleanup')
    if args.enable_cleanup and os.environ.get('ALLOW_STAGING_CLEANUP') != '1':
        parser.error('--enable-cleanup also requires ALLOW_STAGING_CLEANUP=1')
    replay_values = (args.replay_file_version, args.replay_requested_by, args.replay_reason)
    if any(value is not None for value in replay_values) and not all(replay_values):
        parser.error('replay requires --replay-file-version, --replay-requested-by and --replay-reason')

    if args.resume_batch:
        with runner_lock():
            require_no_pending_cleanup(args.resume_batch)
            require_no_pending_replacement(args.resume_batch)
            result = run_downstream(args.resume_batch)
            if args.enable_cleanup:
                clean_transient_storage(args.resume_batch)
        print(json.dumps({'batch_id': args.resume_batch, 'result': result}, default=str))
        return 0

    months = month_rows(args.from_month, args.to_month)
    if not months:
        raise ValueError('No catalog months in requested scope')
    if len(months) != 1 and not args.enable_cleanup:
        raise RuntimeError('Multiple files are disabled until safe staging cleanup is verified')
    if args.replay_file_version is not None and len(months) != 1:
        raise RuntimeError('Replay is limited to exactly one catalog month')
    disk_evidence = check_disk_safety(args.expected_peak_bytes)
    print(json.dumps({'disk_preflight': disk_evidence}), flush=True)
    if len(months) == 24:
        validate_v2_approval(args.v2_approval_file)
    with runner_lock():
        require_no_pending_cleanup()
        require_no_pending_replacement()
        run_files(args, months)
    return 0


def run_files(args, months):
    run_id = args.continue_run
    completed = set()
    if run_id:
        children = execute("""
            SELECT f.source_year_month, f.status, f.batch_id
            FROM control.etl_backfill_file f JOIN control.etl_backfill_run r USING (backfill_run_id)
            WHERE f.backfill_run_id = %(run)s AND r.status = 'RUNNING'
            ORDER BY f.source_year_month;
        """, {'run': run_id}, fetch=True)
        if [child['source_year_month'] for child in children] != months:
            raise RuntimeError('Requested months do not match the incomplete parent')
        for child in children:
            if child['status'] == 'SUCCESS':
                completed.add(child['source_year_month'])
            elif child['batch_id'] is not None:
                raise RuntimeError('Recover linked batch {} before continuing'.format(child['batch_id']))
    else:
        run_id = create_backfill_run(months)
    for position, month in enumerate(months, start=1):
        if month in completed:
            continue
        require_no_pending_cleanup()
        batch_id = None
        try:
            print('[{}/{}] Processing {}'.format(position, len(months), month), flush=True)
            update_backfill_file(run_id, month, 'RUNNING')
            ingestion = run_ingestion(
                month, args.replay_file_version,
                args.replay_requested_by, args.replay_reason,
            )
            batch_id = int(ingestion['batch_id'])
            update_backfill_file(run_id, month, 'RUNNING', batch_id=batch_id)
            if ingestion['process_file_count'] == 0:
                mark_skipped_downstream_success(batch_id)
            else:
                run_downstream(batch_id)
            if not args.no_cleanup:
                clean_transient_storage(batch_id)
        except Exception as exception:
            execute("""
                UPDATE control.etl_backfill_file SET status = 'FAILED', error_message = %(error)s,
                    completed_at = clock_timestamp()
                WHERE backfill_run_id = %(run)s AND source_year_month = %(month)s
                  AND status <> 'SUCCESS';
            """, {'run': run_id, 'month': month, 'error': str(exception)[:2000]})
            raise
    print('Backfill run {} completed {} child file(s); parent success remains gated'.format(
        run_id, len(months)), flush=True)


if __name__ == '__main__':
    raise SystemExit(main())

