"""Static contract checks for the additive recovery migration."""
from pathlib import Path
import unittest


class BackfillRecoverySQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = (Path(__file__).resolve().parents[1] / 'sql/migrations/006_backfill_recovery_and_cleanup.sql').read_text(encoding='utf-8')

    def test_failed_resume_is_locked_and_requires_preserved_staging(self):
        self.assertIn("pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'))", self.sql)
        self.assertIn("v_batch.error_code IS NOT DISTINCT FROM 'DOWNSTREAM_FAILED'", self.sql)
        self.assertIn("v_file.file_status <> 'STAGED'", self.sql)
        self.assertIn('v_foreign_count <> 0', self.sql)

    def test_batch_start_and_cleanup_share_writer_lock_contract(self):
        functions = (Path(__file__).resolve().parents[1] / 'sql/control/003_control_functions.sql').read_text(encoding='utf-8')
        self.assertIn("pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'))", functions)

    def test_global_gate_tracks_children_cleanup_steps_and_balance(self):
        for token in ('validated_file_count', 'successful_child_count',
                      'cleanup_success_count', 'required_step_failure_count',
                      'is_balanced'):
            self.assertIn(token, self.sql)
        self.assertIn("count(*) FILTER (WHERE step.is_required) <> 7", self.sql)

    def test_manual_final_validation_requires_evidence(self):
        self.assertIn('record_backfill_validation', self.sql)
        self.assertIn("p_evidence = '{}'::jsonb", self.sql)

    def test_replay_requires_exact_identity_and_success_history(self):
        self.assertIn('authorize_file_replay', self.sql)
        self.assertIn("v_file.file_status <> 'SKIPPED_ALREADY_PROCESSED'", self.sql)
        self.assertIn('v.file_version_id = p_expected_file_version_id', self.sql)
        self.assertIn('file_reconciliation_history', self.sql)
        self.assertIn("'FILE_REPLAY_AUTHORIZED'", self.sql)


if __name__ == '__main__':
    unittest.main()
