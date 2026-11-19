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
        context = {'batch_id': 44, 'files': [{'etl_file_id': 55}]}
        with patch.object(runner, 'execute', side_effect=[[{'prior': 1}], None]) as execute:
            with self.assertRaisesRegex(RuntimeError, 'Replacement blocked'):
                runner.require_safe_file_version(context)
        self.assertIn('SOURCE_VERSION_REPLACEMENT_BLOCKED', execute.call_args.args[0])
        self.assertEqual(execute.call_args.args[1]['batch'], 44)

    def test_running_resume_refuses_missing_staging_before_dbt(self):
        runner = load_runner()
        with patch.object(runner, 'execute', side_effect=[
            [{'status': 'DBT_RUNNING'}], RuntimeError('staging is absent')
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
                return [{'status': 'FAILED'}]
            if 'resume_failed_downstream' in sql:
                return None
            return []

        with patch.object(runner, 'execute', side_effect=execute), \
             patch.object(runner, 'control_step'), patch.object(runner, 'run_dbt'), \
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

        with patch.object(runner, 'execute', return_value=[{'status': 'STAGED'}]), \
             patch.object(runner, 'control_step'), patch.object(runner, 'run_dbt'), \
             patch.object(runner, 'validate_reconciliation'), \
             patch.object(runner, 'persist_reconciliation', side_effect=persist), \
             patch.object(runner, 'connection', return_value=connection):
            with self.assertRaisesRegex(RuntimeError, 'commit failed'):
                runner.run_downstream(123)

        self.assertEqual(cursor.execute.call_count, 2)
        self.assertIs(connection.__exit__.call_args.args[0], RuntimeError)

    def test_olap_failure_never_persists_history(self):
        runner = load_runner()

        def run_dbt(args, batch):
            if 'tag:olap' in args and '--select' in args:
                raise RuntimeError('OLAP failed')

        with patch.object(runner, 'execute', return_value=[{'status': 'STAGED'}]), \
             patch.object(runner, 'control_step'), \
             patch.object(runner, 'validate_reconciliation'), \
             patch.object(runner, 'run_dbt', side_effect=run_dbt), \
             patch.object(runner, 'connection') as connection:
            with self.assertRaisesRegex(RuntimeError, 'OLAP failed'):
                runner.run_downstream(123)
            connection.assert_not_called()

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

    def test_skipped_file_never_commits_success(self):
        runner = load_runner()
        with patch.object(runner, 'execute', side_effect=AssertionError('database touched')):
            with self.assertRaisesRegex(RuntimeError, 'refusing to fabricate'):
                runner.mark_skipped_downstream_success(123)

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
            [{'status': 'SUCCESS', 'cleanup_status': 'PENDING'}],
            [{'foreign_rows': 0, 'active_batches': 0}],
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
            [{'status': 'SUCCESS', 'cleanup_status': 'PENDING'}],
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

    def test_parent_success_requires_global_24_file_gate(self):
        runner = load_runner()
        captured = {}

        def execute(sql, params=None, fetch=False):
            captured['sql'] = sql
            return []

        with patch.object(runner, 'execute', side_effect=execute):
            with self.assertRaisesRegex(RuntimeError, 'finalization gates'):
                runner.finalize_backfill(123, True)
        self.assertIn('expected_file_count = 24', captured['sql'])
        self.assertIn('cleanup_success_count = 24', captured['sql'])
        self.assertIn('g.source_rows = 89892322', captured['sql'])

    def test_finalize_cli_does_not_require_ingestion_flags(self):
        runner = load_runner()
        with patch.object(sys, 'argv', ['runner', '--finalize-run', '44']), \
             patch.object(runner, 'load_dotenv'), \
             patch.object(runner, 'finalize_backfill') as finalize:
            self.assertEqual(runner.main(), 0)
        finalize.assert_called_once_with(44, True)


if __name__ == '__main__':
    unittest.main()
