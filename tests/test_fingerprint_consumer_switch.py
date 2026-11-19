"""The registry replacement remains explicit and disabled by default."""
from pathlib import Path
import unittest


class FingerprintConsumerSwitch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1]
        cls.project = (root / 'dbt_project/dbt_project.yml').read_text(encoding='utf-8')
        cls.classified = (root / 'dbt_project/models/intermediate/int_taxi_trip_classified.sql').read_text(encoding='utf-8')
        cls.matches = (root / 'dbt_project/models/audit/cross_file_fingerprint_match.sql').read_text(encoding='utf-8')

    def test_default_keeps_legacy_registry(self):
        self.assertIn('use_fingerprint_history_candidate: false', self.project)
        for sql in (self.classified, self.matches):
            self.assertIn("var('use_fingerprint_history_candidate', false)", sql)
            self.assertIn("source('audit', 'trip_fingerprint_history')", sql)
            self.assertIn("source('audit', 'v_trip_fingerprint_history')", sql)


if __name__ == '__main__':
    unittest.main()
