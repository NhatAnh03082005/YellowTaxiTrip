"""Run task 4.4 unit and project-input integration tests with unittest."""
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def main():
    loader = unittest.TestLoader()
    suite = loader.discover(
        str(PROJECT_ROOT / 'mage_project/tests'),
        pattern='test_task_4_4*.py',
        top_level_dir=str(PROJECT_ROOT),
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
