"""Bounded streaming inside the loader; only JSON manifest crosses Mage blocks."""
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .canonical import canonicalize_dataframe
from .parquet_utils import make_source_record_key,calculate_file_sha256
from .profiling import phase


RAW_UNIFIED_COLUMNS = {
    'vendorid': 'raw_vendor_id',
    'tpep_pickup_datetime': 'raw_pickup_datetime',
    'tpep_dropoff_datetime': 'raw_dropoff_datetime',
    'passenger_count': 'raw_passenger_count',
    'trip_distance': 'raw_trip_distance',
    'ratecodeid': 'raw_rate_code_id',
    'store_and_fwd_flag': 'raw_store_and_fwd_flag',
    'pulocationid': 'raw_pu_location_id',
    'dolocationid': 'raw_do_location_id',
    'payment_type': 'raw_payment_type_id',
    'fare_amount': 'raw_fare_amount',
    'extra': 'raw_extra',
    'mta_tax': 'raw_mta_tax',
    'tip_amount': 'raw_tip_amount',
    'tolls_amount': 'raw_tolls_amount',
    'improvement_surcharge': 'raw_improvement_surcharge',
    'total_amount': 'raw_total_amount',
    'congestion_surcharge': 'raw_congestion_surcharge',
    'airport_fee': 'raw_airport_fee',
    'cbd_congestion_fee': 'raw_cbd_congestion_fee',
}


def _unified_frame(raw, canonical):
    """Build one physical slim-v2 row while preserving both logical views."""
    result = canonical.copy(deep=False)
    lineage = {'file_version_id', 'source_row_number', 'source_record_key'}
    canonical_columns = set(result.columns)
    for source in raw.columns:
        normalized = source.strip().lower()
        target = RAW_UNIFIED_COLUMNS.get(normalized)
        if target:
            result[target] = raw[source]
        elif source not in lineage and source not in canonical_columns:
            raise ValueError('Unsupported source column for slim-v2: {}'.format(source))
    for target in RAW_UNIFIED_COLUMNS.values():
        if target not in result:
            result[target] = None
    integer_raw = {
        'raw_vendor_id','raw_passenger_count','raw_rate_code_id',
        'raw_pu_location_id','raw_do_location_id','raw_payment_type_id',
    }
    timestamp_raw = {'raw_pickup_datetime','raw_dropoff_datetime'}
    numeric_raw = set(RAW_UNIFIED_COLUMNS.values()) - integer_raw - timestamp_raw - {'raw_store_and_fwd_flag'}
    for column in integer_raw:
        values = pd.to_numeric(result[column], errors='coerce')
        bad = values.notna() & ((values % 1 != 0) | (values < -(2**63)) | (values >= 2**63))
        result[column] = values.mask(bad).astype('Int64')
    for column in timestamp_raw:
        result[column] = pd.to_datetime(result[column], errors='coerce')
    for column in numeric_raw:
        values = pd.to_numeric(result[column], errors='coerce')
        result[column] = values.where(values.isna() | np.isfinite(values))
    required_lineage = lineage | {'technical_valid', 'cast_error_flags', 'raw_payload'}
    missing = required_lineage - set(result.columns)
    if missing:
        raise ValueError('Slim-v2 unified frame is missing {}'.format(sorted(missing)))
    return result


def prepare_manifest(context, batch_size):
    batch_size=int(batch_size)
    if batch_size<=0: raise ValueError('Positive streaming batch size required')
    return {**context,'stage_load_mode':'direct','prepare_mode':'streaming','source_batch_size':batch_size,'chunks':[],'metrics_pending':True,
            'metrics':{'rows_raw':sum(int(f['parquet_row_count']) for f in context['files']),
                       'rows_canonical':-1,'rows_technical_quarantined':-1}}


def iter_parts(context):
    batch=int(context['batch_id'])
    size=int(context['source_batch_size'])
    for f in context['files']:
        path=Path(f['resolved_path'])
        before=path.stat()
        offset=0
        for part in pq.ParquetFile(path).iter_batches(batch_size=size,use_threads=True):
            with phase('stream_read_and_lineage',batch_id=batch):
                table=pa.Table.from_batches([part])
                numbers=range(offset+1,offset+table.num_rows+1)
                table=table.append_column('source_row_number',pa.array(numbers,type=pa.int64()))
                table=table.append_column('source_record_key',pa.array([make_source_record_key(f['source_file_sha256'],n) for n in numbers]))
                values={'batch_id':batch,'file_version_id':int(f['file_version_id']),
                        'source_file':f['file_name'],'source_year_month':f['source_year_month'],
                        'source_file_sha256':f['source_file_sha256'],'schema_fingerprint':f['schema_fingerprint'],
                        'schema_version':f['schema_version'],'source_last_modified':f['last_modified']}
                for name,value in values.items():
                    table=table.append_column(name,pa.array([value]*table.num_rows))
                raw=table.to_pandas()
            with phase('stream_canonicalize',batch_id=batch):
                canonical,rejected=canonicalize_dataframe(raw)
            offset+=len(raw)
            counts={'source_file':f['file_name'],'row_count':len(raw),
                    'canonical_rows':len(canonical),'technical_reject_rows':len(rejected)}
            yield raw,canonical,rejected,counts
        if offset!=int(f['parquet_row_count']): raise ValueError('Streaming footer mismatch')
        after=path.stat()
        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
            raise ValueError('Source changed while streaming')
        with phase('stream_source_recheck',batch_id=batch):
            if calculate_file_sha256(path)!=f['source_file_sha256']:
                raise ValueError('Source checksum changed while streaming')


def iter_unified_parts(context):
    """Yield bounded slim-v2 frames for one continuous physical COPY.

    Repeated batch/file/schema strings stay in control tables and are joined by
    compatibility views.  Only the immutable version id and row identity cross
    the Python/PostgreSQL boundary for every trip.
    """
    batch = int(context['batch_id'])
    size = int(context['source_batch_size'])
    for f in context['files']:
        path = Path(f['resolved_path'])
        before = path.stat()
        offset = 0
        for part in pq.ParquetFile(path).iter_batches(batch_size=size, use_threads=True):
            with phase('stream_read_and_lineage', batch_id=batch, storage='slim-v2'):
                table = pa.Table.from_batches([part])
                numbers = range(offset + 1, offset + table.num_rows + 1)
                table = table.append_column('source_row_number', pa.array(numbers, type=pa.int64()))
                table = table.append_column(
                    'source_record_key',
                    pa.array([make_source_record_key(f['source_file_sha256'], n) for n in numbers]),
                )
                table = table.append_column(
                    'file_version_id',
                    pa.array([int(f['file_version_id'])] * table.num_rows, type=pa.int64()),
                )
                raw = table.to_pandas()
            with phase('stream_canonicalize', batch_id=batch, storage='slim-v2'):
                accepted, rejected, canonical = canonicalize_dataframe(raw, include_all=True)
                unified = _unified_frame(raw, canonical)
            offset += len(raw)
            counts = {
                'source_file': f['file_name'],
                'row_count': len(raw),
                'canonical_rows': len(accepted),
                'technical_reject_rows': len(rejected),
            }
            yield unified, counts
        if offset != int(f['parquet_row_count']):
            raise ValueError('Streaming footer mismatch')
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError('Source changed while streaming')
        with phase('stream_source_recheck', batch_id=batch, storage='slim-v2'):
            if calculate_file_sha256(path) != f['source_file_sha256']:
                raise ValueError('Source checksum changed while streaming')
