from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from utils.database import execute
from utils.decorators import transformer
from utils.parquet_utils import make_source_record_key

@transformer
def load_source_files(ingestion_context, *args, **kwargs):
    work_root = Path(
        kwargs.get('work_root', '/home/src/data/work/task_4_4')
    ).resolve()
    batch_root = (work_root / str(ingestion_context['batch_id']) / 'raw').resolve()
    # Guard the cleanup below so a bad runtime value can never remove anything
    # outside the configured work directory.
    batch_root.relative_to(work_root)
    if batch_root.exists():
        shutil.rmtree(batch_root)
    batch_root.mkdir(parents=True, exist_ok=True)
    batch_size = int(kwargs.get('source_batch_size', 100000))
    if batch_size <= 0:
        raise ValueError('source_batch_size must be positive')

    chunks = []
    part_number = 0
    for file_number, file_context in enumerate(
        ingestion_context['files'], start=1
    ):
        parquet_file = pq.ParquetFile(file_context['resolved_path'])
        row_offset = 0
        for file_part_index, record_batch in enumerate(
            parquet_file.iter_batches(batch_size=batch_size, use_threads=True)
        ):
            table = pa.Table.from_batches([record_batch])
            row_count = table.num_rows
            row_numbers = list(range(row_offset + 1, row_offset + row_count + 1))
            record_keys = [
                make_source_record_key(file_context['source_file_sha256'], number)
                for number in row_numbers
            ]

            lineage_columns = (
                ('source_row_number', pa.array(row_numbers, type=pa.int64())),
                ('source_record_key', pa.array(record_keys, type=pa.string())),
                ('batch_id', pa.array(
                    [int(ingestion_context['batch_id'])] * row_count,
                    type=pa.int64(),
                )),
                ('file_version_id', pa.array(
                    [int(file_context['file_version_id'])] * row_count,
                    type=pa.int64(),
                )),
                ('source_file', pa.array(
                    [str(file_context['file_name'])] * row_count,
                    type=pa.string(),
                )),
                ('source_year_month', pa.array(
                    [str(file_context['source_year_month'])] * row_count,
                    type=pa.string(),
                )),
                ('source_file_sha256', pa.array(
                    [str(file_context['source_file_sha256'])] * row_count,
                    type=pa.string(),
                )),
                ('schema_fingerprint', pa.array(
                    [str(file_context['schema_fingerprint'])] * row_count,
                    type=pa.string(),
                )),
                ('schema_version', pa.array(
                    [str(file_context['schema_version'])] * row_count,
                    type=pa.string(),
                )),
                ('source_last_modified', pa.array(
                    [str(file_context['last_modified'])] * row_count,
                    type=pa.string(),
                )),
            )
            for column_name, values in lineage_columns:
                table = table.append_column(column_name, values)

            output_path = batch_root / '{}_part{:05d}.parquet'.format(
                Path(file_context['file_name']).stem, file_part_index
            )
            pq.write_table(table, output_path, compression='snappy')
            chunks.append({
                'raw_path': str(output_path), 'row_count': row_count,
                # Kept for compatibility with downstream metadata consumers.
                'row_group_index': file_part_index,
                'source_part_index': file_part_index,
                'etl_file_id': file_context['etl_file_id'],
                'source_file': file_context['file_name'],
            })
            row_offset += row_count
            part_number += 1
            if part_number % 10 == 0:
                execute(
                    'SELECT control.touch_batch_heartbeat(%(batch_id)s);',
                    {'batch_id': int(ingestion_context['batch_id'])},
                )
                print(
                    'Loaded {} bounded parts; latest file rows: {}'.format(
                        part_number, row_offset
                    )
                )
        if row_offset != int(file_context['parquet_row_count']):
            raise ValueError('Row count mismatch for {}'.format(file_context['file_name']))
        print(
            'Completed source file {}/{}: {} ({} rows)'.format(
                file_number,
                len(ingestion_context['files']),
                file_context['file_name'],
                row_offset,
            )
        )
    return {**ingestion_context, 'chunks': chunks}
