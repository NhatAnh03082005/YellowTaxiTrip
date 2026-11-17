from pathlib import Path

import pyarrow.parquet as pq

from utils.parquet_utils import make_source_record_key

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def load_source_files(context, *args, **kwargs):
    work_root = Path(kwargs.get('work_root', '/home/src/data/work/task_4_4'))
    batch_root = work_root / str(context['batch_id']) / 'raw'
    batch_root.mkdir(parents=True, exist_ok=True)
    chunks = []
    for file_context in context['files']:
        parquet_file = pq.ParquetFile(file_context['resolved_path'])
        row_offset = 0
        for group_index in range(parquet_file.num_row_groups):
            dataframe = parquet_file.read_row_group(group_index).to_pandas()
            row_numbers = range(row_offset + 1, row_offset + len(dataframe) + 1)
            dataframe['source_row_number'] = list(row_numbers)
            dataframe['source_record_key'] = [
                make_source_record_key(file_context['source_file_sha256'], number)
                for number in dataframe['source_row_number']
            ]
            dataframe['batch_id'] = int(context['batch_id'])
            dataframe['source_file'] = file_context['file_name']
            dataframe['source_year_month'] = file_context['source_year_month']
            dataframe['source_file_sha256'] = file_context['source_file_sha256']
            dataframe['schema_fingerprint'] = file_context['schema_fingerprint']
            dataframe['schema_version'] = file_context['schema_version']
            dataframe['source_last_modified'] = file_context['last_modified']
            output_path = batch_root / '{}_rg{:04d}.parquet'.format(
                Path(file_context['file_name']).stem, group_index
            )
            dataframe.to_parquet(output_path, index=False)
            chunks.append({
                'raw_path': str(output_path), 'row_count': len(dataframe),
                'row_group_index': group_index, 'etl_file_id': file_context['etl_file_id'],
                'source_file': file_context['file_name'],
            })
            row_offset += len(dataframe)
        if row_offset != int(file_context['parquet_row_count']):
            raise ValueError('Row count mismatch for {}'.format(file_context['file_name']))
    return {**context, 'chunks': chunks}
