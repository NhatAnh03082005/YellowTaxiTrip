import json

import pandas as pd


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
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    if hasattr(value, 'item'):
        return value.item()
    return value


def canonicalize_dataframe(raw_dataframe):
    """Canonicalize one bounded row-group DataFrame and split rejects."""
    raw = raw_dataframe.copy()
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
            'batch_id', 'source_file', 'source_year_month',
            'source_file_sha256', 'source_row_number', 'source_record_key',
            'schema_fingerprint', 'schema_version', 'source_last_modified',
        }
    ]
    raw_payloads = []
    error_flags = []

    for index in canonical.index:
        errors = {}
        payload = {
            column: _json_value(raw.at[index, column])
            for column in source_columns
        }
        raw_payloads.append(payload)

        for column in ('pickup_datetime', 'dropoff_datetime'):
            original = canonical.at[index, column]
            converted = pd.to_datetime(original, errors='coerce')
            if pd.notna(original) and pd.isna(converted):
                errors[column] = 'INVALID_TIMESTAMP'
            canonical.at[index, column] = converted

        for column in INTEGER_COLUMNS:
            if column not in canonical.columns:
                canonical[column] = pd.NA
            original = canonical.at[index, column]
            converted = pd.to_numeric(original, errors='coerce')
            if pd.notna(original) and pd.isna(converted):
                errors[column] = 'INVALID_INTEGER'
            canonical.at[index, column] = converted

        for column in NUMERIC_COLUMNS:
            if column not in canonical.columns:
                canonical[column] = pd.NA
            original = canonical.at[index, column]
            converted = pd.to_numeric(original, errors='coerce')
            if pd.notna(original) and pd.isna(converted):
                errors[column] = 'INVALID_NUMERIC'
            canonical.at[index, column] = converted

        flag = canonical.at[index, 'store_and_fwd_flag'] \
            if 'store_and_fwd_flag' in canonical.columns else None
        if pd.notna(flag):
            flag = str(flag).strip().upper()
            canonical.at[index, 'store_and_fwd_flag'] = (
                flag if flag in {'Y', 'N'} else None
            )

        for column in REQUIRED_TECHNICAL_COLUMNS:
            if column not in canonical.columns or pd.isna(canonical.at[index, column]):
                errors.setdefault(column, 'REQUIRED_VALUE_NULL_OR_UNCASTABLE')
        error_flags.append(errors)

    canonical['raw_payload'] = raw_payloads
    canonical['cast_error_flags'] = error_flags
    rejected_mask = canonical['cast_error_flags'].map(bool)
    accepted = canonical.loc[~rejected_mask].copy()
    rejected = canonical.loc[rejected_mask].copy()
    if len(raw) != len(accepted) + len(rejected):
        raise AssertionError('Technical row reconciliation failed')
    return accepted, rejected


def serialize_json_columns(dataframe):
    result = dataframe.copy()
    for column in ('raw_payload', 'cast_error_flags'):
        if column in result.columns:
            result[column] = result[column].map(
                lambda value: json.dumps(value, ensure_ascii=False, default=str)
            )
    return result
