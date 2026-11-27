import json

import pandas as pd
import numpy as np


COLUMN_MAPPING = {
    'vendorid': 'vendor_id',
    'tpep_pickup_datetime': 'pickup_datetime',
    'tpep_dropoff_datetime': 'dropoff_datetime',
    'passenger_count': 'passenger_count',
    'trip_distance': 'trip_distance',
    'ratecodeid': 'rate_code_id',
    'store_and_fwd_flag': 'store_and_fwd_flag',
    'pulocationid': 'pu_location_id',
    'dolocationid': 'do_location_id',
    'payment_type': 'payment_type_id',
    'fare_amount': 'fare_amount',
    'extra': 'extra',
    'mta_tax': 'mta_tax',
    'tip_amount': 'tip_amount',
    'tolls_amount': 'tolls_amount',
    'improvement_surcharge': 'improvement_surcharge',
    'total_amount': 'total_amount',
    'congestion_surcharge': 'congestion_surcharge',
    'airport_fee': 'airport_fee',
    'cbd_congestion_fee': 'cbd_congestion_fee',
}

INTEGER_COLUMNS = [
    'vendor_id', 'passenger_count', 'rate_code_id',
    'pu_location_id', 'do_location_id', 'payment_type_id',
]
NUMERIC_COLUMNS = [
    'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
    'tolls_amount', 'improvement_surcharge', 'total_amount',
    'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee',
]
REQUIRED_TECHNICAL_COLUMNS = [
    'pickup_datetime', 'dropoff_datetime', 'pu_location_id',
    'do_location_id', 'total_amount',
]


def _json_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return str(value)
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    if hasattr(value, 'item'):
        return value.item()
    return value


def canonicalize_dataframe(raw_dataframe, include_all=False):
    """Vectorized canonicalization for one bounded batch.

    ``include_all`` is used by the slim-v2 loader.  It returns the canonical
    projection for every source row in addition to the accepted/rejected
    partitions, allowing Raw and Canonical to be published from one physical
    COPY without changing either logical contract.
    """
    raw = raw_dataframe.reset_index(drop=True).copy()
    rename_mapping = {
        column: COLUMN_MAPPING[column.strip().lower()]
        for column in raw.columns
        if column.strip().lower() in COLUMN_MAPPING
    }
    canonical = raw.rename(columns=rename_mapping).copy()
    if 'cbd_congestion_fee' not in canonical.columns:
        canonical['cbd_congestion_fee'] = pd.NA

    source_columns = [
        column for column in raw.columns
        if column not in {
            'batch_id', 'file_version_id', 'source_file', 'source_year_month',
            'source_file_sha256', 'source_row_number', 'source_record_key',
            'schema_fingerprint', 'schema_version', 'source_last_modified',
        }
    ]
    row_count = len(canonical)
    error_flags = [{} for _ in range(row_count)]

    def add_errors(mask, column, error_code, overwrite=False):
        for position in mask.to_numpy().nonzero()[0]:
            if overwrite or column not in error_flags[position]:
                error_flags[position][column] = error_code

    for column in ('pickup_datetime', 'dropoff_datetime'):
        if column not in canonical.columns:
            canonical[column] = pd.NaT
            continue
        original = canonical[column]
        converted = pd.to_datetime(original, errors='coerce')
        add_errors(
            original.notna() & converted.isna(),
            column,
            'INVALID_TIMESTAMP',
        )
        canonical[column] = converted

    for column in INTEGER_COLUMNS:
        if column not in canonical.columns:
            canonical[column] = pd.NA
            continue
        original = canonical[column]
        converted = pd.to_numeric(original, errors='coerce')
        add_errors(
            original.notna() & converted.isna(),
            column,
            'INVALID_INTEGER',
        )
        bad = converted.notna() & (~np.isfinite(converted) | (converted % 1 != 0) | (converted < -32768) | (converted > 32767))
        add_errors(bad, column, 'INTEGER_OUT_OF_RANGE_OR_FRACTIONAL')
        canonical[column] = converted.mask(bad)

    for column in NUMERIC_COLUMNS:
        if column not in canonical.columns:
            canonical[column] = pd.NA
            continue
        original = canonical[column]
        converted = pd.to_numeric(original, errors='coerce')
        add_errors(
            original.notna() & converted.isna(),
            column,
            'INVALID_NUMERIC',
        )
        scale, limit = (3, 10**7) if column == 'trip_distance' else (2, 10**10)
        bad = converted.notna() & (~np.isfinite(converted) | (converted.round(scale).abs() >= limit))
        add_errors(bad, column, 'NUMERIC_OUT_OF_RANGE_OR_NONFINITE')
        canonical[column] = converted.mask(bad)

    if 'store_and_fwd_flag' not in canonical.columns:
        canonical['store_and_fwd_flag'] = pd.NA
    else:
        normalized_flags = (
            canonical['store_and_fwd_flag']
            .astype('string')
            .str.strip()
            .str.upper()
        )
        add_errors(canonical['store_and_fwd_flag'].notna() & ~normalized_flags.isin(['Y', 'N']), 'store_and_fwd_flag', 'INVALID_FLAG')
        canonical['store_and_fwd_flag'] = normalized_flags.where(
            normalized_flags.isin(['Y', 'N']), pd.NA
        )

    rejected_mask = pd.Series(False, index=canonical.index)
    for column in REQUIRED_TECHNICAL_COLUMNS:
        missing = canonical[column].isna()
        add_errors(
            missing,
            column,
            'REQUIRED_VALUE_NULL_OR_UNCASTABLE',
        )
        rejected_mask |= missing

    canonical['cast_error_flags'] = error_flags
    canonical['raw_payload'] = None
    # Preserve original values for every row with a cast/required-field error.
    # Healthy rows remain compact and rejected rows remain fully recoverable.
    error_indices = pd.Series(error_flags, index=canonical.index).map(bool)
    if error_indices.any():
        payload_frame = raw.loc[error_indices, source_columns].copy()
        payload_frame = payload_frame.astype(object).where(pd.notna(payload_frame), None)
        payloads = [
            {column: _json_value(value) for column, value in row.items()}
            for row in payload_frame.to_dict(orient='records')
        ]
        canonical.loc[error_indices, 'raw_payload'] = pd.Series(
            payloads, index=canonical.index[error_indices], dtype='object'
        )

    accepted = canonical.loc[~rejected_mask].copy()
    rejected = canonical.loc[rejected_mask].copy()

    if len(raw) != len(accepted) + len(rejected):
        raise AssertionError('Technical row reconciliation failed')
    if include_all:
        all_rows = canonical.copy()
        all_rows['technical_valid'] = ~rejected_mask
        return accepted, rejected, all_rows
    return accepted, rejected


def serialize_json_columns(dataframe):
    result = dataframe.copy()
    for column in ('raw_payload', 'cast_error_flags'):
        if column in result.columns:
            result[column] = result[column].map(
                lambda value: None if value is None else json.dumps(
                    value, ensure_ascii=False, default=str
                )
            )
    return result
