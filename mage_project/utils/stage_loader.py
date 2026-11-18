import json
import os
import re
from io import StringIO
import uuid
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text


IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
INTEGER_TYPES = re.compile(r'\b(smallint|integer|bigint)\b')


def postgres_engine():
    password = quote_plus(os.environ['POSTGRES_PASSWORD'])
    url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        os.environ['POSTGRES_USER'], password, os.environ['POSTGRES_HOST'],
        os.environ.get('POSTGRES_PORT', '5432'), os.environ['POSTGRES_DB'],
    )
    return create_engine(url)


def _safe_identifier(value):
    if not IDENTIFIER.match(value):
        raise ValueError('Unsafe SQL identifier: {}'.format(value))
    return value


def serialize_json_value(value):
    """Keep missing values as SQL NULL; serialize real JSON values only."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if not isinstance(value, (dict, list)):
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            pass
    return json.dumps(value, ensure_ascii=False, default=str)


def normalize_copy_frame(dataframe, target_type_names):
    """Format nullable integer columns as `1`, never pandas' float `1.0`."""
    result = dataframe.copy()
    for column, type_name in target_type_names.items():
        if column not in result.columns or not INTEGER_TYPES.search(type_name.lower()):
            continue
        original = result[column]
        numeric = pd.to_numeric(original, errors='coerce')
        invalid = original.notna() & numeric.isna()
        fractional = numeric.notna() & ((numeric % 1) != 0)
        if invalid.any() or fractional.any():
            bad_values = original.loc[invalid | fractional].head(5).tolist()
            raise ValueError(
                'Column {} contains non-integer values for {}: {}'.format(
                    column, type_name, bad_values
                )
            )
        result[column] = numeric.astype('Int64')
    return result


def upsert_dataframe(dataframe, schema, table, conflict_column='source_record_key'):
    """Insert the intersection of DataFrame/target columns idempotently."""
    if dataframe.empty:
        return 0
    schema = _safe_identifier(schema)
    table = _safe_identifier(table)
    conflict_column = _safe_identifier(conflict_column)
    engine = postgres_engine()
    target_columns = {
        item['name']: item for item in inspect(engine).get_columns(table, schema=schema)
    }
    common = [column for column in dataframe.columns if column in target_columns]
    if conflict_column not in common:
        raise RuntimeError(
            '{}.{} must expose {} for idempotent loading'.format(
                schema, table, conflict_column
            )
        )
    target_type_names = {
        column: str(target_columns[column]['type']) for column in common
    }
    frame = normalize_copy_frame(dataframe[common], target_type_names)
    json_columns = []
    for column in common:
        type_name = str(target_columns[column]['type']).lower()
        if 'json' in type_name:
            json_columns.append(column)
            frame[column] = frame[column].map(serialize_json_value)
    temporary = '_mage_{}_{}'.format(table, uuid.uuid4().hex[:12])
    quoted_columns = ', '.join('"{}"'.format(column) for column in common)
    buffer = StringIO()
    frame.to_csv(
        buffer, index=False, header=False, na_rep='\\N', lineterminator='\n'
    )
    buffer.seek(0)
    with engine.begin() as connection:
        # The temporary table inherits exact target types. PostgreSQL COPY is
        # substantially faster and uses less Python memory than multi-value
        # INSERT for 100k-row work parts.
        connection.execute(text(
            'CREATE TEMP TABLE "{}" ON COMMIT DROP AS '
            'SELECT {} FROM "{}"."{}" WITH NO DATA'.format(
                temporary, quoted_columns, schema, table
            )
        ))
        cursor = connection.connection.cursor()
        cursor.copy_expert(
            'COPY "{}" ({}) FROM STDIN WITH (FORMAT CSV, NULL \'\\N\')'.format(
                temporary, quoted_columns
            ),
            buffer,
        )
        connection.execute(text(
            'INSERT INTO "{}"."{}" ({}) SELECT {} FROM "{}" '
            'ON CONFLICT ("{}") DO NOTHING'.format(
                schema, table, quoted_columns, quoted_columns, temporary,
                conflict_column
            )
        ))
    return len(frame)
