"""Guard the lifecycle macro against regressions to whole-Fact scans."""
from pathlib import Path
import re
import unittest


class FactLifecycleScope(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = (Path(__file__).resolve().parents[1] / 'dbt_project/macros/fact_identity_materialization.sql').read_text(encoding='utf-8')

    def test_guard_archive_and_baseline_are_version_scoped(self):
        self.assertIn("where f.file_version_id in (' ~ processing_versions ~ ')", self.sql)
        self.assertGreaterEqual(self.sql.count("f.file_version_id in (' ~ processing_versions ~ ')"), 3)
        self.assertIn("from {{ source('stage', 'yellow_taxi_raw') }}", self.sql)
        self.assertIn('where batch_id = {{ load_batch_id | int }}', self.sql)

    def test_no_unscoped_fact_delete(self):
        deletes = re.findall(r"delete from ' ~ target_relation.*", self.sql)
        self.assertEqual(len(deletes), 2)
        self.assertTrue(any("using ' ~ temp_stale" in delete for delete in deletes))
        self.assertIn("where f.file_version_id = s.old_version_id returning f.*", self.sql)


if __name__ == '__main__':
    unittest.main()
