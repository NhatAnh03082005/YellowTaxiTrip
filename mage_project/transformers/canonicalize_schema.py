from pathlib import Path
import shutil

import pandas as pd
import pyarrow.parquet as pq

from utils.canonical import canonicalize_dataframe, serialize_json_columns
from utils.database import execute
if 'transformer' not in globals():
    from utils.decorators import transformer

@transformer
def canonicalize_schema(ingestion_context, *args, **kwargs):
    batch_root = Path(kwargs.get('work_root', '/home/src/data/work/task_4_4')) / str(
        ingestion_context['batch_id']
    )
    canonical_root = batch_root / 'canonical'
    reject_root = batch_root / 'technical_reject'
    for output_root in (canonical_root, reject_root):
        if output_root.exists():
            shutil.rmtree(output_root)
    canonical_root.mkdir(parents=True, exist_ok=True)
    reject_root.mkdir(parents=True, exist_ok=True)
    batch_size = int(kwargs.get('canonical_batch_size', 100000))
    if batch_size <= 0:
        raise ValueError('canonical_batch_size must be positive')
    processed = []
    part_number = 0
    for chunk_number, chunk in enumerate(ingestion_context['chunks'], start=1):
        parquet_file = pq.ParquetFile(chunk['raw_path'])
        chunk_rows = 0
        for record_batch in parquet_file.iter_batches(batch_size=batch_size):
            raw = record_batch.to_pandas()
            canonical, rejected = canonicalize_dataframe(raw)
            stem = '{}_part{:05d}'.format(
                Path(chunk['raw_path']).stem, part_number
            )
            canonical_path = canonical_root / '{}.parquet'.format(stem)
            reject_path = reject_root / '{}.parquet'.format(stem)
            serialize_json_columns(canonical).to_parquet(
                canonical_path, index=False
            )
            serialize_json_columns(rejected).to_parquet(
                reject_path, index=False
            )
            processed.append({
                **chunk,
                'raw_path': chunk['raw_path'],
                'canonical_path': str(canonical_path),
                'reject_path': str(reject_path),
                'row_count': len(raw),
                'canonical_rows': len(canonical),
                'technical_reject_rows': len(rejected),
                'canonical_part_number': part_number,
            })
            chunk_rows += len(raw)
            part_number += 1
            if part_number % 10 == 0:
                execute(
                    'SELECT control.touch_batch_heartbeat(%(batch_id)s);',
                    {'batch_id': int(ingestion_context['batch_id'])},
                )
                print(
                    'Canonicalized {} batches ({} rows in latest source chunk)'.format(
                        part_number, chunk_rows
                    )
                )
        if chunk_rows != int(chunk['row_count']):
            raise ValueError(
                'Canonical row-count mismatch for {}: expected {}, read {}'.format(
                    chunk['raw_path'], chunk['row_count'], chunk_rows
                )
            )
        print(
            'Completed raw chunk {}/{}: {}'.format(
                chunk_number, len(ingestion_context['chunks']), chunk['raw_path']
            )
        )
    return {**ingestion_context, 'chunks': processed}
