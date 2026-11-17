import json
import os
import re
import uuid
from urllib.parse import quote_plus

from sqlalchemy import create_engine, inspect, text


IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


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
    frame = dataframe[common].copy()
    json_columns = []
    for column in common:
        type_name = str(target_columns[column]['type']).lower()
        if 'json' in type_name:
            json_columns.append(column)
            frame[column] = frame[column].map(
                lambda value: json.dumps(value, ensure_ascii=False, default=str)
                if not isinstance(value, str) else value
            )
    temporary = '_mage_{}_{}'.format(table, uuid.uuid4().hex[:12])
    quoted_columns = ', '.join('"{}"'.format(column) for column in common)
    select_columns = ', '.join(
        '"{}"::jsonb'.format(column) if column in json_columns
        else '"{}"'.format(column)
        for column in common
    )
    with engine.begin() as connection:
        frame.to_sql(temporary, connection, schema='public', if_exists='fail', index=False,
                     method='multi', chunksize=2000)
        connection.execute(text(
            'INSERT INTO "{}"."{}" ({}) SELECT {} FROM "public"."{}" '
            'ON CONFLICT ("{}") DO NOTHING'.format(
                schema, table, quoted_columns, select_columns, temporary, conflict_column
            )
        ))
        connection.execute(text('DROP TABLE "public"."{}"'.format(temporary)))
    return len(frame)
