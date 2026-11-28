"""Exercise the real conflict guard in a named disposable PostgreSQL QA only."""
import os
from pathlib import Path
import unittest

import jinja2
import psycopg2


@unittest.skipUnless(os.getenv('DDS_SQL_QA_DATABASE'), 'Dedicated SQL QA database is required')
class MinimalFactConflictSQL(unittest.TestCase):
    def test_conflict_rolls_back_statement_and_retry_preserves_identity(self):
        database = os.environ['DDS_SQL_QA_DATABASE']
        if not database.startswith('dbt_flow_fixture_'):
            raise ValueError('Only disposable dbt_flow_fixture_* databases are allowed')
        root = Path(__file__).resolve().parents[1]
        macros = jinja2.Environment().from_string((
            root / 'dbt_project/macros/minimal_fact_version_guard.sql'
        ).read_text(encoding='utf-8')).make_module()
        with psycopg2.connect(
            host=os.getenv('DBT_HOST', '127.0.0.1'),
            port=os.getenv('DBT_PORT', '5433'), database=database,
            user=os.environ['POSTGRES_USER'], password=os.getenv('POSTGRES_PASSWORD', ''),
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute('''create temporary table version_guard_fixture(
                    trip_key bigint generated always as identity primary key,
                    source_record_key text unique not null,
                    file_version_id bigint not null, value text not null)''')
                cursor.execute(str(macros.minimal_create_fact_version_guard()))
                cursor.execute("insert into version_guard_fixture(source_record_key,file_version_id,value) values('old',1,'protected')")
                cursor.execute('''do $$ begin
                  begin
                    insert into version_guard_fixture as f(source_record_key,file_version_id,value)
                    values('first_new',2,'new'),('old',2,'collision')
                    on conflict(source_record_key) do update set
                      file_version_id=pg_temp.minimal_fact_version_guard(f.file_version_id,excluded.file_version_id),
                      value=excluded.value
                    where row(f.file_version_id,f.value) is distinct from row(excluded.file_version_id,excluded.value);
                    raise exception 'Expected collision did not fail';
                  exception when raise_exception then
                    if sqlerrm not like 'Source identity collision across file versions;%' then raise; end if;
                  end;
                end $$''')
                cursor.execute('select trip_key,source_record_key,file_version_id,value from version_guard_fixture')
                self.assertEqual(cursor.fetchall(), [(1, 'old', 1, 'protected')])
                statement = '''insert into version_guard_fixture as f(source_record_key,file_version_id,value)
                    values('old',1,'updated') on conflict(source_record_key) do update set
                      file_version_id=pg_temp.minimal_fact_version_guard(f.file_version_id,excluded.file_version_id),
                      value=excluded.value
                    where row(f.file_version_id,f.value) is distinct from row(excluded.file_version_id,excluded.value)'''
                cursor.execute(statement)
                cursor.execute('select trip_key,file_version_id,value,xmin::text from version_guard_fixture')
                before = cursor.fetchall()
                self.assertEqual(before[0][:3], (1, 1, 'updated'))
                cursor.execute(statement)
                self.assertEqual(cursor.rowcount, 0)
                cursor.execute('select trip_key,file_version_id,value,xmin::text from version_guard_fixture')
                self.assertEqual(cursor.fetchall(), before)
                cursor.execute('drop function pg_temp.minimal_fact_version_guard(bigint,bigint)')
            connection.rollback()


if __name__ == '__main__':
    unittest.main()
