"""Run the approved source-version/retry control fixture without retaining QA rows."""
from pathlib import Path
import subprocess
import unittest


class ControlLifecycleRollback(unittest.TestCase):
    def test_version_approval_and_retry_idempotency(self):
        root = Path(__file__).resolve().parents[1]
        functions = (root / 'sql/control/003_control_functions.sql').read_text(encoding='utf-8')
        functions = '\n'.join(line for line in functions.splitlines() if not line.lstrip().startswith('\\'))
        source = (root / 'tests/control/test_file_lifecycle.sql').read_text(encoding='utf-8')
        source = '\n'.join(line for line in source.splitlines() if not line.lstrip().startswith('\\'))
        source = source.replace("'qa_43_t18_file'", "'qa_43_t18_file_rollback'")
        source = source.replace('yellow_tripdata_2025-02.parquet', 'yellow_tripdata_2095-02.parquet')
        source = source.replace("'2025-02'", "'2095-02'")
        source = source.replace("'2025-03-01 00:00:00+00'", "'2095-03-01 00:00:00+00'")
        source = source.replace("'2025-03-02 00:00:00+00'", "'2095-03-02 00:00:00+00'")
        sql = 'BEGIN;\n' + functions + '\n' + source + '\nROLLBACK;\n'
        result = subprocess.run(
            ['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
            cwd=root, input=sql, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('PASS T18', result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
