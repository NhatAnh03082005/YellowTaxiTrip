import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock, patch
from collections import namedtuple
from contextlib import nullcontext


def load_runner():
    # Safety gates must be testable without a database driver or connection.
    psycopg = types.ModuleType('psycopg2')
    extras = types.ModuleType('psycopg2.extras')
    extras.RealDictCursor = object
    psycopg.extras = extras
    path = Path(__file__).resolve().parents[1] / 'deployment/scripts/run_24_month_backfill.py'
    with patch.dict(sys.modules, {'psycopg2': psycopg, 'psycopg2.extras': extras}):
        spec = importlib.util.spec_from_file_location('backfill_safety_runner', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class BackfillSafetyGates(unittest.TestCase):
    def test_explicit_qa_host_and_port_survive_dotenv(self):
        runner = load_runner()
        with patch.dict(runner.os.environ, {'POSTGRES_HOST': '127.0.0.1',
                                           'POSTGRES_HOST_PORT': '15439'}, clear=True), \
             patch.object(Path, 'read_text', return_value='POSTGRES_HOST=postgres\nPOSTGRES_PORT=5432'):
            runner.load_dotenv()
            self.assertEqual(runner.os.environ['POSTGRES_HOST'], '127.0.0.1')
            self.assertEqual(runner.os.environ['POSTGRES_PORT'], '15439')
            self.assertEqual(runner.os.environ['DBT_HOST'], '127.0.0.1')

    def test_host_controller_defaults_to_localhost_for_docker_dotenv(self):
        runner = load_runner()
        with patch.dict(runner.os.environ, {}, clear=True), \
             patch.object(Path, 'read_text', return_value='POSTGRES_HOST=postgres\nPOSTGRES_HOST_PORT=5433'):
            runner.load_dotenv()
            self.assertEqual(runner.os.environ['POSTGRES_HOST'], 'localhost')
            self.assertEqual(runner.os.environ['POSTGRES_PORT'], '5433')

    def test_acknowledging_committed_replacement_skips_all_writes(self):
        runner = load_runner()
        with patch.object(runner, 'execute', return_value=[{'status': 'SUCCESS', 'replacement': True}]) as execute, \
             patch.object(runner, 'run_dbt') as dbt, patch.object(runner, 'connection') as connection:
            self.assertEqual(runner.run_downstream(44), {'batch_id': 44, 'already_committed': True})
        execute.assert_called_once()
        dbt.assert_not_called()
        connection.assert_not_called()

    def test_replacement_is_blocked_before_staging_until_retirement_is_safe(self):
        runner = load_runner()
        context = {'batch_id': 44, 'files': [{'etl_file_id': 55,
                   'file_name': 'qa.parquet', 'source_file_sha256': 'b'*64,
                   'file_version_id': 77}]}
        with patch.object(runner, 'execute', side_effect=[
            [{'source_file_sha256': 'a'*64}], [], None]) as execute:
            with self.assertRaisesRegex(RuntimeError, 'Replacement blocked'):
                runner.require_safe_file_version(context)
        self.assertIn('SOURCE_VERSION_REPLACEMENT_BLOCKED', execute.call_args.args[0])
        self.assertEqual(execute.call_args.args[1]['batch'], 44)

    def test_approved_replacement_is_bound_to_exact_checksum_and_prior(self):
        runner = load_runner()
        context = {'batch_id': 44, 'files': [{'etl_file_id': 55,
                   'file_name': 'qa.parquet', 'source_file_sha256': 'b'*64,
                   'file_version_id': 77}]}
        with patch.object(runner, 'execute', side_effect=[
            [{'source_file_sha256': 'a'*64}], [{'etl_file_id': 33}]]) as execute:
            runner.require_safe_file_version(context)
        self.assertEqual(context['replacement_approval_file_id'], 33)
        self.assertEqual(execute.call_args.args[1]['previous_sha'], 'a'*64)

    def test_running_resume_refuses_missing_staging_before_dbt(self):
        runner = load_runner()
        with patch.object(runner, 'execute', side_effect=[
            [{'status': 'DBT_RUNNING', 'file_version_id': 77}], RuntimeError('staging is absent')
        ]) as execute, patch.object(runner, 'run_dbt') as dbt:
            with self.assertRaisesRegex(RuntimeError, 'staging is absent'):
                runner.run_downstream(44)
        self.assertIn('resume_failed_downstream', execute.call_args.args[0])
        dbt.assert_not_called()

    def test_cleanup_retry_does_not_run_downstream(self):
        runner = load_runner()
        with patch.object(sys, 'argv', ['runner', '--retry-cleanup', '44']), \
             patch.object(runner, 'load_dotenv'), \
             patch.object(runner, 'runner_lock', return_value=nullcontext()), \
             patch.object(runner, 'run_downstream') as downstream, \
             patch.object(runner, 'clean_transient_storage') as cleanup:
            self.assertEqual(runner.main(), 0)
        cleanup.assert_called_once_with(44)
        downstream.assert_not_called()

    def test_pending_cleanup_blocks_new_parent(self):
        runner = load_runner()
        with patch.object(sys, 'argv', ['runner', '--no-cleanup', '--expected-peak-bytes', '1']), \
             patch.object(runner, 'load_dotenv'), \
             patch.object(runner, 'month_rows', return_value=['2024-05']), \
             patch.object(runner, 'check_disk_safety', return_value={}), \
             patch.object(runner, 'runner_lock', return_value=nullcontext()), \
             patch.object(runner, 'execute', return_value=[{'batch_id': 44}]), \
             patch.object(runner, 'create_backfill_run') as parent:
            with self.assertRaisesRegex(RuntimeError, 'Cleanup pending for batch 44'):
                runner.main()
        parent.assert_not_called()

    def test_disk_gate_preserves_25_percent_after_forecast(self):
        runner = load_runner()
        usage = namedtuple('usage', 'total used free')(1000, 400, 600)
        with patch.object(runner.shutil, 'disk_usage', return_value=usage):
            evidence = runner.check_disk_safety(300)
            self.assertEqual(evidence['projected_free_bytes'], 300)
            with self.assertRaisesRegex(RuntimeError, '25% reserve'):
                runner.check_disk_safety(351)
            with self.assertRaisesRegex(RuntimeError, 'positive'):
                runner.check_disk_safety(0)

    def test_full_backfill_requires_feature_gate_and_two_approved_owners(self):
        runner = load_runner()
        with patch.dict(runner.os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, 'ALLOW_24_MONTH_BACKFILL'):
                runner.validate_v2_approval('approval.md')

    def test_replay_requires_verified_exact_file_version(self):
        runner = load_runner()
        context = {'files': [{
            'action': 'SKIP_ALREADY_PROCESSED', 'file_version_id': 77,
            'etl_file_id': 12,
        }]}
        with patch.object(runner, 'execute') as execute:
            replay = runner.authorize_replay(context, 77, 'qa_owner', 'FIN-09 fixture')
        self.assertEqual(replay['files'][0]['action'], 'REPROCESS_IDEMPOTENTLY')
        self.assertEqual(replay['replay_file_version_id'], 77)
        self.assertIn('authorize_file_replay', execute.call_args.args[0])
        with self.assertRaisesRegex(RuntimeError, 'does not match'):
            runner.authorize_replay(context, 78, 'qa_owner', 'wrong version')

    def test_failed_downstream_uses_database_resume_gate(self):
        runner = load_runner()
        calls = []

        def execute(sql, params=None, fetch=False):
            calls.append(sql)
            if 'SELECT b.status' in sql:
                return [{'status': 'FAILED', 'file_version_id': 77}]
            if 'resume_failed_downstream' in sql:
                return None
            return []

        with patch.object(runner, 'execute', side_effect=execute), \
             patch.object(runner, 'control_step'), patch.object(runner, 'run_dbt'), \
             patch.object(runner, 'ensure_reference_initialized'), \
             patch.object(runner, 'validate_reconciliation'), \
             patch.object(runner, 'persist_reconciliation', return_value={}), \
             patch.object(runner, 'connection', return_value=MagicMock()):
            runner.run_downstream(123)

        self.assertTrue(any('resume_failed_downstream' in sql for sql in calls))

    def test_commit_failure_exits_reconciliation_transaction_with_error(self):
        runner = load_runner()
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value

        def fail_commit(sql, params):
            if 'commit_batch_success' in sql:
                raise RuntimeError('commit failed')

        cursor.execute.side_effect = fail_commit

        def persist(batch_id, query):
            query('INSERT history', {'batch': batch_id})
            return {}

        with patch.object(runner, 'execute', return_value=[{'status': 'STAGED', 'file_version_id': 77}]), \
             patch.object(runner, 'control_step'), patch.object(runner, 'run_dbt'), \
             patch.object(runner, 'ensure_reference_initialized'), \
             patch.object(runner, 'validate_reconciliation'), \
             patch.object(runner, 'persist_reconciliation', side_effect=persist), \
             patch.object(runner, 'connection', return_value=connection):
            with self.assertRaisesRegex(RuntimeError, 'commit failed'):
                runner.run_downstream(123)

        self.assertEqual(cursor.execute.call_count, 2)
        self.assertIs(connection.__exit__.call_args.args[0], RuntimeError)

    def test_monthly_invocations_exclude_olap_and_use_batch_tests(self):
        runner = load_runner()
        with patch.object(runner, 'execute', return_value=[{'status': 'STAGED', 'file_version_id': 77}]), \
             patch.object(runner, 'control_step'), \
             patch.object(runner, 'validate_reconciliation'), \
             patch.object(runner, 'run_dbt') as dbt, \
             patch.object(runner, 'ensure_reference_initialized'), \
             patch.object(runner, 'persist_reconciliation', return_value={}), \
             patch.object(runner, 'connection', return_value=MagicMock()):
            runner.run_downstream(123)
        commands = [call.args[0] for call in dbt.call_args_list]
        self.assertEqual(commands, [
            ['run', '--selector', 'minimal_dds_run'],
            ['test', '--selector', 'minimal_dds_batch_tests'],
        ])

    def test_failed_batch_test_cannot_commit_watermark(self):
        runner = load_runner()
        def dbt(args, batch, extra):
            if args[0] == 'test':
                raise RuntimeError('batch test failed')
        with patch.object(runner, 'execute', return_value=[{'status': 'STAGED', 'file_version_id': 77}]) as execute, \
             patch.object(runner, 'control_step'), \
             patch.object(runner, 'ensure_reference_initialized'), \
             patch.object(runner, 'run_dbt', side_effect=dbt), \
             patch.object(runner, 'connection') as connection:
            with self.assertRaisesRegex(RuntimeError, 'batch test failed'):
                runner.run_downstream(123)
        connection.assert_not_called()
        self.assertTrue(any('fail_batch' in call.args[0] for call in execute.call_args_list))

    def test_skip_in_parent_does_not_run_dds_or_cleanup(self):
        runner = load_runner()
        args = types.SimpleNamespace(continue_run=None, replay_file_version=None,
                                     replay_requested_by=None, replay_reason=None, no_cleanup=False)
        with patch.object(runner, 'create_backfill_run', return_value=9), \
             patch.object(runner, 'require_no_pending_cleanup'), \
             patch.object(runner, 'update_backfill_file'), \
             patch.object(runner, 'run_ingestion', return_value={
                 'batch_id': 44, 'process_file_count': 0, 'skipped_file': {'file_version_id': 77}}), \
             patch.object(runner, 'record_verified_skip') as record, \
             patch.object(runner, 'run_downstream') as downstream, \
             patch.object(runner, 'clean_transient_storage') as cleanup:
            runner.run_files(args, ['2024-01'])
        record.assert_called_once()
        downstream.assert_not_called()
        cleanup.assert_not_called()

    def test_reference_seed_only_when_missing(self):
        runner = load_runner()
        with patch.object(runner, 'execute', return_value=[{'missing': False}]), \
             patch.object(runner, 'run_dbt') as dbt:
            runner.ensure_reference_initialized(44, 77)
        dbt.assert_not_called()
        with patch.object(runner, 'execute', return_value=[{'missing': True}]), \
             patch.object(runner, 'run_dbt') as dbt:
            runner.ensure_reference_initialized(44, 77)
        self.assertEqual(dbt.call_args.args[0], ['seed'])

    def test_reconciliation_checks_each_money_field(self):
        runner = load_runner()
        row = {'file_version_id': 77, 'source_row_delta': 0, 'trusted_row_delta': 0}
        for field in ('fare', 'tip', 'total'):
            for suffix in ('source_{}_eligible_delta', 'trusted_{}_eligible_delta',
                           'source_{}_delta', 'trusted_{}_delta'):
                row[suffix.format(field)] = 0
        with patch.object(runner, 'execute', return_value=[row]):
            self.assertEqual(runner.validate_reconciliation(44), [row])
            row['trusted_tip_delta'] = 1
            with self.assertRaisesRegex(RuntimeError, 'tip reconciliation failed'):
                runner.validate_reconciliation(44)

    def test_legacy_archive_variables_are_rejected(self):
        runner = load_runner()
        with patch.object(runner, 'subprocess') as subprocess:
            with self.assertRaisesRegex(ValueError, 'unsupported'):
                runner.run_dbt(['run'], 44, {'fact_lifecycle_mode': 'archive'})
        subprocess.run.assert_not_called()

    def test_dbt_uses_explicit_isolated_profile(self):
        runner = load_runner()
        with patch.dict(runner.os.environ, {'DBT_PROFILES_DIR': 'qa_profile'}), \
             patch.object(runner.subprocess, 'run') as process:
            runner.run_dbt(['run', '--selector', 'minimal_dds_run'], 44,
                           {'load_file_version_id': 77})
        command = process.call_args.args[0]
        self.assertEqual(command[command.index('--profiles-dir') + 1], 'qa_profile')
        self.assertIn('--target-path', command)
        self.assertIn('--log-path', command)

    def test_parent_without_validated_child_cannot_finalize(self):
        runner = load_runner()
        with patch.object(runner, 'execute', return_value=[]):
            with self.assertRaisesRegex(RuntimeError, 'finalization gates'):
                runner.finalize_backfill(123, True)

    def test_cleanup_never_calls_database(self):
        runner = load_runner()
        with patch.object(runner, 'execute', side_effect=AssertionError('database touched')):
            with self.assertRaisesRegex(RuntimeError, 'cleanup disabled'):
                runner.clean_transient_storage(123)

    def test_old_skip_shortcut_is_not_available(self):
        runner = load_runner()
        self.assertFalse(hasattr(runner, 'mark_skipped_downstream_success'))

    def test_multiple_files_rejected_before_backfill_created(self):
        runner = load_runner()
        with patch.object(sys, 'argv', ['runner', '--no-cleanup']), \
             patch.object(runner, 'load_dotenv'), \
             patch.object(runner, 'month_rows', return_value=['2024-01', '2024-02']), \
             patch.object(runner, 'create_backfill_run', side_effect=AssertionError('run created')):
            with self.assertRaisesRegex(RuntimeError, 'Multiple files'):
                runner.main()

    def test_cleanup_requires_environment_gate(self):
        runner = load_runner()
        with patch.dict(runner.os.environ, {}, clear=True), \
             patch.object(runner, 'connection', side_effect=AssertionError('database touched')):
            with self.assertRaisesRegex(RuntimeError, 'ALLOW_STAGING_CLEANUP'):
                runner.clean_transient_storage(123)

    def test_cleanup_checkpoints_and_truncates_only_single_batch(self):
        runner = load_runner()
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [
            [{'status': 'SUCCESS', 'cleanup_status': 'PENDING', 'file_version_id': 77}],
            [{'foreign_rows': 0, 'active_batches': 0}],
            [{'batch_id': 123, 'file_version_id': 77, 'valid': True}],
        ]
        outside = []

        def execute(sql, params=None, fetch=False):
            outside.append(sql)
            return []

        with patch.dict(runner.os.environ, {'ALLOW_STAGING_CLEANUP': '1'}), \
             patch.object(runner, 'connection', return_value=connection), \
             patch.object(runner, 'execute', side_effect=execute), \
             patch.object(runner.Path, 'is_dir', return_value=False):
            runner.clean_transient_storage(123)

        transaction_sql = ' '.join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn('foreign_rows', transaction_sql)
        self.assertIn("cleanup_status = 'RUNNING'", transaction_sql)
        self.assertIn('TRUNCATE TABLE stage.yellow_taxi_raw', transaction_sql)
        self.assertTrue(any("cleanup_status = 'SUCCESS'" in sql for sql in outside))

    def test_cleanup_rejects_foreign_batch_and_records_failure(self):
        runner = load_runner()
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [
            [{'status': 'SUCCESS', 'cleanup_status': 'PENDING', 'file_version_id': 77}],
            [{'foreign_rows': 1, 'active_batches': 0}],
        ]
        outside = []

        def execute(sql, params=None, fetch=False):
            outside.append(sql)
            return []

        with patch.dict(runner.os.environ, {'ALLOW_STAGING_CLEANUP': '1'}), \
             patch.object(runner, 'connection', return_value=connection), \
             patch.object(runner, 'execute', side_effect=execute):
            with self.assertRaisesRegex(RuntimeError, 'another batch'):
                runner.clean_transient_storage(123)
        self.assertFalse(any('TRUNCATE TABLE' in call.args[0] for call in cursor.execute.call_args_list))
        self.assertTrue(any("cleanup_status = 'FAILED'" in sql for sql in outside))

    def test_parent_success_requires_all_requested_files_but_no_olap_gate(self):
        runner = load_runner()
        captured = {}

        def execute(sql, params=None, fetch=False):
            captured['sql'] = sql
            return []

        with patch.object(runner, 'execute', side_effect=execute):
            with self.assertRaisesRegex(RuntimeError, 'finalization gates'):
                runner.finalize_backfill(123, True)
        self.assertIn('g.validated_file_count = expected_file_count', captured['sql'])
        self.assertIn('g.cleanup_success_count = expected_file_count', captured['sql'])
        self.assertNotIn("olap_bi_validation_status = 'SUCCESS'", captured['sql'])

    def test_finalize_cli_does_not_require_ingestion_flags(self):
        runner = load_runner()
        with patch.object(sys, 'argv', ['runner', '--finalize-run', '44']), \
             patch.object(runner, 'load_dotenv'), \
             patch.object(runner, 'finalize_backfill') as finalize:
            self.assertEqual(runner.main(), 0)
        finalize.assert_called_once_with(44, True)


if __name__ == '__main__':
    unittest.main()
