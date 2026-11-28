"""Validate block discovery using Mage's executor without running data loads."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


@unittest.skipUnless(importlib.util.find_spec('mage_ai'), 'Requires the Mage runtime')
class MageBlockRegistrationTests(unittest.TestCase):
    def test_pipeline_blocks_register_with_the_mage_executor(self):
        from mage_ai.data_preparation.models.pipeline import Pipeline

        repo = Path(__file__).resolve().parents[2]
        checked = 0
        for metadata in sorted((repo / 'pipelines').glob('*/metadata.yaml')):
            pipeline = Pipeline.get(metadata.parent.name, repo_path=str(repo), use_repo_path=True)
            for block in pipeline.blocks_by_uuid.values():
                with self.subTest(pipeline=pipeline.uuid, block=block.uuid):
                    # Use real source evaluation, injected decorators and function
                    # validation; intercept the function call before any I/O.
                    with patch.object(block, 'execute_block_function', return_value=None) as execute:
                        block._execute_block(
                            input_vars=[None] * len(block.upstream_blocks),
                            outputs_from_input_vars={}, global_vars={},
                        )
                    execute.assert_called_once()
                    self.assertEqual(execute.call_args.args[0].__name__, block.uuid)
                    checked += 1
        self.assertGreater(checked, 0)


if __name__ == '__main__':
    unittest.main()
