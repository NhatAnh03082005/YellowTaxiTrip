import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor


@contextmanager
def postgres_connection():
    connection = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=int(os.environ.get('POSTGRES_PORT', '5432')),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        application_name='mage_phase2_4_4',
    )
    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def fetch_one(query, parameters=None):
    with postgres_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, parameters or {})
            row = cursor.fetchone()
        connection.commit()
    return dict(row) if row else None


def fetch_all(query, parameters=None):
    with postgres_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, parameters or {})
            rows = cursor.fetchall()
        connection.commit()
    return [dict(row) for row in rows]


def execute(query, parameters=None):
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters or {})
        connection.commit()


def require_tables(qualified_names):
    """Fail early when a 4.5 physical table has not been deployed."""
    missing = []
    for qualified_name in qualified_names:
        row = fetch_one(
            'SELECT to_regclass(%(name)s) AS relation_name;',
            {'name': qualified_name},
        )
        if not row or row['relation_name'] is None:
            missing.append(qualified_name)
    if missing:
        raise RuntimeError(
            'Missing Physical Staging tables from task 4.5: '
            + ', '.join(missing)
        )
