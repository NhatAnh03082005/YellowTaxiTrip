import hashlib
import json

import pyarrow.parquet as pq


REQUIRED_COLUMNS = {
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'pulocationid',
    'dolocationid',
    'total_amount',
}


def calculate_file_sha256(file_path, chunk_size=8 * 1024 * 1024):
    digest = hashlib.sha256()
    with open(file_path, 'rb') as stream:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def inspect_parquet(file_path):
    parquet_file = pq.ParquetFile(file_path)
    if parquet_file.metadata.num_rows <= 0:
        raise ValueError('Parquet contains no rows')
    schema = parquet_file.schema_arrow
    lower_names = {name.lower() for name in schema.names}
    missing = sorted(REQUIRED_COLUMNS - lower_names)
    if missing:
        raise ValueError('Missing required columns: {}'.format(missing))
    contract = [
        {'ordinal': index, 'name': field.name, 'logical_type': str(field.type)}
        for index, field in enumerate(schema)
    ]
    serialized = json.dumps(
        contract, ensure_ascii=False, separators=(',', ':')
    )
    fingerprint = hashlib.sha256(serialized.encode('utf-8')).hexdigest()
    return {
        'row_count': parquet_file.metadata.num_rows,
        'row_group_count': parquet_file.num_row_groups,
        'schema_fingerprint': fingerprint,
        'schema_contract': contract,
    }


def make_source_record_key(file_sha256, row_number):
    preimage = '{}:{}'.format(file_sha256.lower(), int(row_number))
    return hashlib.sha256(preimage.encode('utf-8')).hexdigest()
