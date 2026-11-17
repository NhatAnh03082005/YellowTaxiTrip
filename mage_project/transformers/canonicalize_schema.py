from pathlib import Path

import pandas as pd

from utils.canonical import canonicalize_dataframe, serialize_json_columns

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def canonicalize_schema(context, *args, **kwargs):
    batch_root = Path(kwargs.get('work_root', '/home/src/data/work/task_4_4')) / str(
        context['batch_id']
    )
    canonical_root = batch_root / 'canonical'
    reject_root = batch_root / 'technical_reject'
    canonical_root.mkdir(parents=True, exist_ok=True)
    reject_root.mkdir(parents=True, exist_ok=True)
    processed = []
    for chunk in context['chunks']:
        raw = pd.read_parquet(chunk['raw_path'])
        canonical, rejected = canonicalize_dataframe(raw)
        stem = Path(chunk['raw_path']).stem
        canonical_path = canonical_root / '{}.parquet'.format(stem)
        reject_path = reject_root / '{}.parquet'.format(stem)
        serialize_json_columns(canonical).to_parquet(canonical_path, index=False)
        serialize_json_columns(rejected).to_parquet(reject_path, index=False)
        processed.append({
            **chunk, 'canonical_path': str(canonical_path),
            'reject_path': str(reject_path), 'canonical_rows': len(canonical),
            'technical_reject_rows': len(rejected),
        })
    return {**context, 'chunks': processed}
