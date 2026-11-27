"""Run the storage-optimized Yellow Taxi pipeline one immutable file at a time.

This is the master controller missing from the Phase 2 Mage-only ingestion DAG.
It reuses the Mage block functions, validates DDS, commits the watermark, then
cleans the single-writer staging workspace. Reporting runs separately.
"""

from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
import csv
from decimal import Decimal
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

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
    controller_host = os.environ.get('POSTGRES_HOST', 'localhost')
    for line in (ROOT / '.env').read_text(encoding='utf-8').splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#') or '=' not in stripped:
            continue
        key, value = stripped.split('=', 1)
        os.environ.setdefault(key, value)
    os.environ['POSTGRES_HOST'] = controller_host
    os.environ['POSTGRES_PORT'] = os.environ.get('POSTGRES_HOST_PORT', os.environ.get('POSTGRES_PORT', '5433'))
    os.environ['DBT_HOST'] = os.environ['POSTGRES_HOST']
    os.environ['DBT_PORT'] = os.environ['POSTGRES_PORT']
    os.environ.setdefault('DBT_PROFILES_DIR', str(ROOT / '.dbt'))


def connection():
    return psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=int(os.environ['POSTGRES_PORT']),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ.get('POSTGRES_PASSWORD', ''),
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


def dbt_executable() -> str:
    for name in ('.venv-dbt-312', '.venv-dbt'):
        candidate = ROOT / name / 'Scripts' / 'dbt.exe'
        if candidate.is_file():
            return str(candidate)
    return 'dbt'


def run_dbt(arguments: list[str], batch_id: int, extra_vars=None) -> None:
    if extra_vars and any(key in extra_vars for key in (
        'fact_lifecycle_mode', 'enable_fingerprint_history_candidate',
        'use_fingerprint_history_candidate',
    )):
        raise ValueError('Legacy archive/candidate-history variables are unsupported by the DDS runner')
    variables = json.dumps({
        'load_batch_id': batch_id,
        'dbt_minimal_mode': True,
        'fact_lifecycle_mode': 'guard',
        **(extra_vars or {}),
    })
    invocation = '{}-{}-{}'.format(batch_id, datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'), arguments[0])
    artifact_root = WORK_ROOT / 'dbt_artifacts' / invocation
    target_path = artifact_root / 'target'
    log_path = artifact_root / 'logs'
    command = [
        dbt_executable(), '--log-path', str(log_path), *arguments,
        '--target-path', str(target_path), '--project-dir', str(DBT_ROOT),
        '--profiles-dir', os.environ.get('DBT_PROFILES_DIR', str(ROOT / '.dbt')),
        '--vars', variables,
    ]
    print(json.dumps({'phase': 'dbt', 'invocation': invocation, 'arguments': arguments,
                      'batch_id': batch_id, 'file_version_id': (extra_vars or {}).get('load_file_version_id'),
                      'sha256': (extra_vars or {}).get('source_file_sha256'),
                      'artifact_root': str(artifact_root)}), flush=True)
    started = time.monotonic()
    try:
        subprocess.run(command, cwd=ROOT, env=os.environ.copy(), check=True)
    finally:
        if arguments[0] == 'test':
            subprocess.run([sys.executable, str(DBT_ROOT / 'scripts' / 'build_with_detailed_tests.py'),
                            '--report-only', '--target-path', str(target_path)],
                           cwd=ROOT, env=os.environ.copy(), check=False)
        print(json.dumps({'phase': 'dbt', 'invocation': invocation,
                          'elapsed_seconds': round(time.monotonic() - started, 3),
                          'artifact_root': str(artifact_root)}), flush=True)


@contextmanager
def timed_phase(name: str, batch_id: int | None, **context):
    started = time.monotonic()
    try:
        yield
    except Exception as exception:
        print(json.dumps({'phase': name, 'batch_id': batch_id, 'status': 'FAILED',
                          **context, 'error': str(exception), 'elapsed_seconds': round(time.monotonic()-started, 3)}), flush=True)
        raise
    else:
        print(json.dumps({'phase': name, 'batch_id': batch_id, 'status': 'SUCCESS',
                          **context, 'elapsed_seconds': round(time.monotonic()-started, 3)}), flush=True)


def ensure_reference_initialized(batch_id: int, version_id: int, source_sha256=None) -> None:
    missing = execute('''SELECT to_regclass('dds.vendor_codes') IS NULL
        OR to_regclass('dds.payment_type_codes') IS NULL
        OR to_regclass('dds.rate_codes') IS NULL
        OR to_regclass('dds.zone_key_map') IS NULL
        OR to_regclass('dds.dq_rule_policy') IS NULL AS missing;''', fetch=True)
    if missing[0]['missing']:
        with timed_phase('REFERENCE_SEEDS', batch_id):
            run_dbt(['seed'], batch_id, {'load_file_version_id': version_id,
                                         'source_file_sha256': source_sha256})


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


def validate_reconciliation(batch_id: int, query=None) -> list:
    query = execute if query is None else query
    rows = query(
        'SELECT * FROM dq.file_reconciliation WHERE batch_id = %(batch)s;',
        {'batch': batch_id}, fetch=True,
    )
    if not rows:
        raise RuntimeError('No reconciliation rows produced for batch {}'.format(batch_id))
    tolerance = Decimal(os.environ.get('AMOUNT_RECONCILIATION_TOLERANCE', '0.01'))
    if tolerance < 0:
        raise RuntimeError('AMOUNT_RECONCILIATION_TOLERANCE must be nonnegative')
    for row in rows:
        if row['source_row_delta'] != 0 or row['trusted_row_delta'] != 0:
            raise RuntimeError('Row reconciliation failed for file version {}'.format(
                row['file_version_id']
            ))
        for field in ('fare', 'tip', 'total'):
            if (row['source_{}_eligible_delta'.format(field)] != 0
                    or row['trusted_{}_eligible_delta'.format(field)] != 0
                    or abs(Decimal(row['source_{}_delta'.format(field)])) > tolerance
                    or abs(Decimal(row['trusted_{}_delta'.format(field)])) > tolerance):
                raise RuntimeError('{} reconciliation failed for file version {}'.format(
                    field, row['file_version_id']))
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
                source_amount_delta, trusted_amount_delta, measure_details, recorded_at
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
                %(source_amount_delta)s, %(trusted_amount_delta)s,
                %(measure_details)s::jsonb, clock_timestamp()
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
                measure_details = EXCLUDED.measure_details,
                recorded_at = EXCLUDED.recorded_at;
            """,
            {
                **row, 'batch_id': batch_id,
                'measure_details': json.dumps(dict(row), default=str),
                'raw_amount_eligible_rows': row['raw_total_eligible_rows'],
                'canonical_amount_eligible_rows': row['canonical_total_eligible_rows'],
                'technical_amount_eligible_rows': row['technical_total_eligible_rows'],
                'fact_amount_eligible_rows': row['fact_total_eligible_rows'],
                'business_amount_eligible_rows': row['business_total_eligible_rows'],
                'duplicate_amount_eligible_rows': row['duplicate_total_eligible_rows'],
                'raw_amount': row['raw_total_amount'],
                'canonical_amount': row['canonical_total_amount'],
                'technical_amount': row['technical_total_amount'],
                'fact_amount': row['fact_total_amount'],
                'business_amount': row['business_total_amount'],
                'duplicate_amount': row['duplicate_total_amount'],
                'source_amount_delta': row['source_total_delta'],
                'trusted_amount_delta': row['trusted_total_delta'],
            },
        )
        query('''UPDATE control.etl_step_run SET details = %(details)s::jsonb
                 WHERE batch_id = %(batch)s AND step_name = 'RECONCILIATION';''',
              {'batch': batch_id, 'details': json.dumps(dict(row), default=str)})
    totals = {
        key: sum(int(row[key]) for row in rows)
        for key in ('raw_rows', 'canonical_rows', 'technical_rows',
                    'fact_rows', 'business_rows', 'duplicate_rows',
                    'raw_total_eligible_rows')
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
         'amount_eligible': totals['raw_total_eligible_rows']},
    )
    return {'rows': rows, 'totals': totals}


def run_downstream(batch_id: int) -> dict:
    state = execute(
        '''SELECT b.status, r.batch_id IS NOT NULL AS replacement,
                  f.file_version_id, f.source_file_sha256
           FROM control.etl_batch b
           LEFT JOIN control.etl_replacement r USING (batch_id)
           LEFT JOIN control.etl_file f USING (batch_id)
           WHERE b.batch_id = %(batch)s;''',
        {'batch': batch_id}, fetch=True,
    )
    if not state:
        raise RuntimeError('Batch {} does not exist'.format(batch_id))
    replacement = state[0].get('replacement', False)
    if state[0]['status'] == 'SUCCESS':
        return {'batch_id': batch_id, 'already_committed': True}
    if state[0]['status'] not in ('FAILED', 'STAGED', 'DBT_RUNNING', 'RECONCILING'):
        raise RuntimeError(
            'Batch {} cannot resume downstream from {}'.format(
                batch_id, state[0]['status']
            )
        )
    version_id = state[0].get('file_version_id')
    if version_id is None:
        raise RuntimeError('Batch {} has no immutable file version'.format(batch_id))
    execute('SELECT control.resume_failed_downstream(%(batch)s);', {'batch': batch_id})
    def dbt(args):
        variables = {'load_file_version_id': int(version_id),
                     'source_file_sha256': state[0].get('source_file_sha256'),
                     'amount_reconciliation_tolerance': float(os.environ.get('AMOUNT_RECONCILIATION_TOLERANCE', '0.01'))}
        if replacement:
            run_dbt(args, batch_id, {**variables, 'apply_approved_fact_replacement': True})
        else:
            run_dbt(args, batch_id, variables)

    try:
        with timed_phase('DBT_BUILD_DDS', batch_id):
            control_step(batch_id, 'DBT_BUILD_DDS', 'start')
            ensure_reference_initialized(batch_id, int(version_id), state[0].get('source_file_sha256'))
            dbt(['run', '--selector', 'minimal_dds_run'])
            control_step(batch_id, 'DBT_BUILD_DDS', 'success')

        with timed_phase('DBT_TEST', batch_id):
            control_step(batch_id, 'DBT_TEST', 'start')
            dbt(['test', '--selector', 'minimal_dds_batch_tests'])
            control_step(batch_id, 'DBT_TEST', 'success')

        with timed_phase('RECONCILIATION', batch_id):
            execute("SELECT control.transition_batch(%(batch)s, 'RECONCILING');", {'batch': batch_id})
            control_step(batch_id, 'RECONCILIATION', 'start')
            validate_reconciliation(batch_id)
            control_step(batch_id, 'RECONCILIATION', 'success')

        with timed_phase('DDS_COMMIT', batch_id), connection() as conn:
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
                    SELECT b.status, f.cleanup_status, f.file_version_id
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
                    LOCK TABLE stage.yellow_taxi_raw, stage.yellow_taxi_canonical,
                        dds.int_taxi_trip_classified IN ACCESS EXCLUSIVE MODE;
                    LOCK TABLE dds.dbt_minimal_workspace_state IN ACCESS EXCLUSIVE MODE;
                """)
                safety = execute_cursor(cursor, """
                    SELECT
                      (SELECT count(*) FROM stage.yellow_taxi_raw
                       WHERE batch_id <> %(batch)s OR file_version_id <> %(version)s)
                      + (SELECT count(*) FROM stage.yellow_taxi_canonical
                         WHERE batch_id <> %(batch)s OR file_version_id <> %(version)s)
                      + (SELECT count(*) FROM dds.int_taxi_trip_classified
                         WHERE batch_id <> %(batch)s OR file_version_id <> %(version)s)
                      AS foreign_rows,
                      (SELECT count(*) FROM control.etl_batch
                       WHERE batch_id <> %(batch)s
                         AND pipeline_name = 'pl_taxi_master'
                         AND status NOT IN ('SUCCESS', 'FAILED', 'CANCELLED'))
                      AS active_batches;
                """, {'batch': batch_id, 'version': rows[0]['file_version_id']}, fetch=True)[0]
                if safety['foreign_rows']:
                    raise RuntimeError('Refusing cleanup: Staging contains another batch')
                if safety['active_batches']:
                    raise RuntimeError('Refusing cleanup: another production batch is active')
                workspace = execute_cursor(cursor, '''
                    SELECT batch_id, file_version_id, valid
                    FROM dds.dbt_minimal_workspace_state
                    WHERE model_name = 'int_taxi_trip_classified';
                ''', fetch=True)
                if (len(workspace) != 1 or workspace[0]['batch_id'] != batch_id
                        or workspace[0]['file_version_id'] != rows[0]['file_version_id']):
                    raise RuntimeError('Refusing cleanup: classification workspace scope differs')
                execute_cursor(cursor, """
                    UPDATE control.etl_backfill_file
                    SET cleanup_status = 'RUNNING', cleanup_error = NULL
                    WHERE batch_id = %(batch)s;
                    UPDATE dds.dbt_minimal_workspace_state SET valid = false
                    WHERE model_name = 'int_taxi_trip_classified';
                    TRUNCATE TABLE stage.yellow_taxi_raw, stage.yellow_taxi_canonical,
                        dds.int_taxi_trip_classified
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
            SELECT prior.file_version_id, prior.source_file_sha256
            FROM control.etl_file current_attempt
            JOIN control.etl_batch current_batch ON current_batch.batch_id = current_attempt.batch_id
            JOIN control.etl_file prior ON prior.source_file = current_attempt.source_file
            JOIN control.etl_batch prior_batch ON prior_batch.batch_id = prior.batch_id
            WHERE current_attempt.etl_file_id = %(file)s
              AND prior.file_status = 'STAGED' AND prior_batch.status = 'SUCCESS'
              AND prior_batch.pipeline_name = current_batch.pipeline_name
              AND prior_batch.source_name = current_batch.source_name
              AND prior.source_year_month = current_attempt.source_year_month
              AND prior.file_version_id <> current_attempt.file_version_id
            ORDER BY prior_batch.ended_at DESC NULLS LAST, prior_batch.batch_id DESC
            LIMIT 1;
        """, {'file': item['etl_file_id']}, fetch=True)
        if previous:
            approvals = execute("""
                SELECT a.etl_file_id FROM control.etl_file a
                WHERE a.source_file = %(file)s
                  AND a.source_file_sha256 = %(sha)s
                  AND a.file_version_id = %(version)s
                  AND a.previous_sha256 = %(previous_sha)s
                  AND a.file_status = 'APPROVED_SOURCE_VERSION'
                  AND a.approval_status = 'APPROVED'
                  AND a.approved_by IS NOT NULL AND a.approved_at IS NOT NULL
                ORDER BY a.etl_file_id DESC LIMIT 1;
            """, {'file': item['file_name'], 'sha': item['source_file_sha256'],
                  'version': item['file_version_id'],
                  'previous_sha': previous[0]['source_file_sha256']}, fetch=True)
            if approvals:
                context['replacement_approval_file_id'] = approvals[0]['etl_file_id']
                continue
            message = 'Replacement blocked until an explicit matching source-version approval exists'
            execute("""
                SELECT control.fail_batch(%(batch)s, 'SOURCE_VERSION_REPLACEMENT_BLOCKED', %(message)s);
            """, {'batch': context['batch_id'], 'message': message})
            raise RuntimeError(message)


def run_ingestion(month: str, replay_file_version_id=None,
                  replay_requested_by=None, replay_reason=None,
                  run_type='BACKFILL') -> dict:
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

    context = get_extraction_window(
        run_type=run_type,
        triggered_by='scheduled_dds_runner' if run_type == 'INCREMENTAL' else 'backfill_controller',
    )
    context = discover_source_files(
        context, catalog_path=str(CATALOG), source_year_month_from=month,
        source_year_month_to=month,
    )
    if run_type == 'INCREMENTAL' and context['files_selected'] == 0:
        try:
            from utils.parquet_utils import calculate_file_sha256
            from utils.paths import resolve_data_path
            with CATALOG.open(newline='', encoding='utf-8') as stream:
                rows = [row for row in csv.DictReader(stream)
                        if row['source_year_month'] == month]
            if len(rows) != 1:
                raise RuntimeError('Incremental skip requires exactly one catalog file')
            row = rows[0]
            path = resolve_data_path(row['local_path'], data_root=str(DATA_ROOT))
            actual_sha = calculate_file_sha256(path)
            if actual_sha != row['sha256'].lower():
                raise RuntimeError('Incremental skip checksum mismatch for {}'.format(row['file_name']))
            versions = execute('''SELECT f.file_version_id FROM control.etl_file f
                JOIN control.etl_batch b USING(batch_id)
                WHERE f.source_file = %(file)s AND f.source_year_month = %(month)s
                  AND f.source_file_sha256 = %(sha)s AND f.file_status = 'STAGED'
                  AND b.status = 'SUCCESS' ORDER BY b.batch_id DESC LIMIT 1''',
                {'file': row['file_name'], 'month': month, 'sha': actual_sha}, fetch=True)
            if not versions:
                raise RuntimeError('Incremental skip has no successful matching file version')
            return {'batch_id': context['batch_id'], 'process_file_count': 0,
                    'skipped_file': {'file_name': row['file_name'],
                                     'source_file_sha256': actual_sha,
                                     'file_version_id': versions[0]['file_version_id']}}
        except Exception as exception:
            execute("SELECT control.fail_batch(%(batch)s, 'INCREMENTAL_SKIP_FAILED', %(error)s)",
                    {'batch': context['batch_id'], 'error': str(exception)[:2000]})
            raise
    context = verify_checksum(
        context, schema_registry_path=str(SCHEMA_REGISTRY), data_root=str(DATA_ROOT),
    )
    if replay_file_version_id is not None:
        context = authorize_replay(
            context, replay_file_version_id, replay_requested_by, replay_reason,
        )
    verified_files = context['files']
    context = filter_incremental(context)
    if context['process_file_count'] == 0:
        if len(verified_files) != 1 or verified_files[0]['action'] != 'SKIP_ALREADY_PROCESSED':
            raise RuntimeError('Skip requires exactly one verified successful file version')
        return {'batch_id': context['batch_id'], 'process_file_count': 0,
                'skipped_file': verified_files[0]}
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
    if context.get('replacement_approval_file_id') is not None:
        execute('SELECT control.bind_approved_replacement(%(batch)s,%(approval)s);',
                {'batch': context['batch_id'],
                 'approval': context['replacement_approval_file_id']})
    return {**staged, 'process_file_count': process_count}


def record_verified_skip(run_id: int, month: str, ingestion: dict) -> None:
    item = ingestion['skipped_file']
    batch_id = int(ingestion['batch_id'])
    with connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            prior = execute_cursor(cursor, '''
                SELECT b.batch_id, f.file_version_id
                FROM control.etl_file f
                JOIN control.etl_batch b USING (batch_id)
                JOIN control.file_reconciliation_history h
                  ON h.file_version_id = f.file_version_id AND h.successful_batch_id = b.batch_id
                JOIN control.etl_backfill_file bf ON bf.batch_id = b.batch_id
                WHERE f.source_file = %(file)s AND f.source_year_month = %(month)s
                  AND f.source_file_sha256 = %(sha)s
                  AND f.file_version_id = %(version)s AND f.file_status = 'STAGED'
                  AND b.status = 'SUCCESS' AND bf.cleanup_status = 'SUCCESS'
                  AND NOT EXISTS (
                    SELECT 1 FROM control.etl_file newer
                    JOIN control.etl_batch newer_batch USING (batch_id)
                    WHERE newer.source_file = f.source_file
                      AND newer.file_version_id <> f.file_version_id
                      AND newer_batch.status = 'SUCCESS'
                      AND newer_batch.batch_id > b.batch_id
                  )
                ORDER BY b.batch_id DESC LIMIT 1 FOR UPDATE OF b;
            ''', {'file': item['file_name'], 'month': month, 'sha': item['source_file_sha256'],
                  'version': item['file_version_id']}, fetch=True)
            if not prior:
                raise RuntimeError('Skip has no cleaned DDS SUCCESS with matching checksum/version')
            execute_cursor(cursor, '''
                UPDATE control.etl_batch SET status = 'CANCELLED', ended_at = clock_timestamp(),
                    error_code = 'ALREADY_PROCESSED', error_message = 'Verified immutable file version already committed'
                WHERE batch_id = %(batch)s AND status = 'RUNNING';
                UPDATE control.etl_backfill_file
                SET status = 'SUCCESS', batch_id = %(prior)s, file_version_id = %(version)s,
                    cleanup_status = 'SUCCESS', cleaned_at = clock_timestamp(),
                    completed_at = clock_timestamp(), error_message = NULL
                WHERE backfill_run_id = %(run)s AND source_year_month = %(month)s;
                UPDATE control.etl_backfill_run r SET completed_file_count = (
                    SELECT count(*) FROM control.etl_backfill_file f
                    WHERE f.backfill_run_id = r.backfill_run_id AND f.status = 'SUCCESS'
                ) WHERE backfill_run_id = %(run)s;
                SELECT control.log_event('pl_taxi_master', 'INFO',
                    'Verified checksum/version already committed; DDS skipped', %(batch)s,
                    p_event_code => 'DDS_VERIFIED_SKIP',
                    p_details => jsonb_build_object('prior_batch_id', %(prior)s,
                        'file_version_id', %(version)s, 'sha256', %(sha)s));
            ''', {'batch': batch_id, 'prior': prior[0]['batch_id'], 'version': item['file_version_id'],
                  'run': run_id, 'month': month, 'sha': item['source_file_sha256']})


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
              expected_file_count > 0 AND completed_file_count = expected_file_count
              AND dds_validation_status = 'SUCCESS'
              AND EXISTS (
                  SELECT 1 FROM control.v_backfill_global_reconciliation g
                  WHERE g.backfill_run_id = %(run)s
                    AND g.validated_file_count = expected_file_count
                    AND g.successful_child_count = expected_file_count
                    AND g.cleanup_success_count = expected_file_count
                    AND g.required_step_failure_count = 0
                    AND g.is_balanced
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
    parser.add_argument('--run-type', choices=('BACKFILL', 'INCREMENTAL'), default='BACKFILL')
    recovery = parser.add_mutually_exclusive_group()
    recovery.add_argument('--resume-batch', type=int)
    recovery.add_argument('--retry-cleanup', type=int)
    recovery.add_argument('--continue-run', type=int)
    recovery.add_argument('--finalize-run', type=int)
    parser.add_argument('--no-cleanup', action='store_true', help='Retain staging (required until cleanup gates pass)')
    parser.add_argument('--enable-cleanup', action='store_true', help='Use gated single-writer cleanup')
    parser.add_argument('--replay-file-version', type=int)
    parser.add_argument('--replay-requested-by')
    parser.add_argument('--replay-reason')
    parser.add_argument('--expected-peak-bytes', type=int)
    parser.add_argument('--v2-approval-file')
    args = parser.parse_args()
    load_dotenv()
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
            with timed_phase('MAGE_INGESTION', None, source_year_month=month):
                ingestion = run_ingestion(
                    month, args.replay_file_version,
                    args.replay_requested_by, args.replay_reason,
                    run_type=getattr(args, 'run_type', 'BACKFILL'),
                )
            batch_id = int(ingestion['batch_id'])
            update_backfill_file(run_id, month, 'RUNNING', batch_id=batch_id)
            if ingestion['process_file_count'] == 0:
                record_verified_skip(run_id, month, ingestion)
            else:
                run_downstream(batch_id)
            if ingestion['process_file_count'] and not args.no_cleanup:
                with timed_phase('CLEANUP', batch_id):
                    clean_transient_storage(batch_id)
        except Exception as exception:
            if batch_id is not None:
                execute('''SELECT control.fail_batch(%(batch)s, 'DDS_RUN_FAILED', %(error)s)
                    WHERE EXISTS (SELECT 1 FROM control.etl_batch
                                  WHERE batch_id = %(batch)s
                                    AND status IN ('RUNNING','STAGED','DBT_RUNNING','RECONCILING'))''',
                    {'batch': batch_id, 'error': str(exception)[:2000]})
            execute("""
                UPDATE control.etl_backfill_file SET status = 'FAILED', error_message = %(error)s,
                    completed_at = clock_timestamp()
                WHERE backfill_run_id = %(run)s AND source_year_month = %(month)s
                  AND status <> 'SUCCESS';
            """, {'run': run_id, 'month': month, 'error': str(exception)[:2000]})
            raise
    if getattr(args, 'run_type', 'BACKFILL') == 'INCREMENTAL':
        execute("""SELECT control.record_backfill_validation(
            %(run)s, 'DDS', 'SUCCESS', 'scheduled_dds_runner',
            jsonb_build_object('mode','INCREMENTAL','file_count',%(count)s,
                               'gate','batch_test_and_reconciliation'));
        """, {'run': run_id, 'count': len(months)})
        finalize_backfill(run_id, True)
        print('Incremental DDS run {} finalized after batch gates'.format(run_id), flush=True)
    else:
        print('Backfill run {} completed {} child file(s); parent success remains gated'.format(
            run_id, len(months)), flush=True)


if __name__ == '__main__':
    raise SystemExit(main())

