"""Exercise partition failure counts using literal scenarios in disposable QA."""
from contextlib import closing
import os
from pathlib import Path
import unittest

import jinja2
import psycopg2


@unittest.skipUnless(os.getenv('DDS_SQL_QA_DATABASE'), 'Dedicated SQL QA database is required')
class MinimalPartitionSQL(unittest.TestCase):
    def test_partition_routes_counts_scope_and_missing_keys(self):
        database = os.environ['DDS_SQL_QA_DATABASE']
        if not database.startswith('dbt_flow_fixture_'):
            raise ValueError('Only disposable dbt_flow_fixture_* databases are allowed')
        names = ['yellow_taxi_raw','yellow_taxi_canonical','technical_quarantine',
                 'int_taxi_trip_classified','fact_taxi_trip','business_quarantine','duplicate_quarantine']
        raw, canonical, technical, classified, fact, business, duplicate = names

        def row(table, key='k', disposition=None, batch=101, version=1):
            return table, key, batch, version, disposition

        def accepted(disposition='ACCEPTED'):
            return [row(raw),row(canonical),row(classified,disposition=disposition),row(fact)]

        source_failure = ('SOURCE_PARTITION',1)
        canonical_failure = ('CANONICAL_CLASSIFIED',1)
        target_failure = ('CLASSIFIED_TARGET',1)
        cases = [
            ('empty', [], []),
            ('accepted', accepted(), []),
            ('warning', accepted('WARNING'), []),
            ('technical', [row(raw),row(technical)], []),
            ('business', [row(raw),row(canonical),row(classified,disposition='BUSINESS_QUARANTINE'),row(business)], []),
            ('duplicate', [row(raw),row(canonical),row(classified,disposition='DUPLICATE'),row(duplicate)], []),
            ('raw_duplicate', accepted()+[row(raw)], [source_failure]),
            ('missing_raw', [r for r in accepted() if r[0]!=raw], [source_failure]),
            ('missing_canonical', [r for r in accepted() if r[0]!=canonical], [source_failure,canonical_failure]),
            ('missing_classification', [r for r in accepted() if r[0]!=classified], [canonical_failure,target_failure]),
            ('missing_fact', [r for r in accepted() if r[0]!=fact], [target_failure]),
            ('extra_technical', accepted()+[row(technical)], [source_failure]),
            ('extra_target', accepted()+[row(business)], [target_failure]),
            ('bad_disposition', accepted('BAD'), [target_failure]),
            ('null_key', [(r[0],None,*r[2:]) for r in accepted()], [source_failure]),
            ('other_staging_batch', accepted()+[row(raw,'other',batch=999)], []),
            ('other_target_version', accepted()+[row(fact,'other',version=2)], []),
            ('orphan_previous_attempt', accepted()+[row(fact,'other',batch=999)], [source_failure,target_failure]),
            ('wrong_target_route', [row(raw),row(canonical),row(classified,disposition='ACCEPTED'),row(business)], [target_failure]),
        ]
        tables = {n:'pg_temp.p_'+n for n in names}
        env = jinja2.Environment()
        env.globals.update(config=lambda **k:'',var=lambda k,d=None:True if k=='dbt_minimal_mode' else d,
                           minimal_batch_id=lambda:101,minimal_version_id=lambda:1,
                           source=lambda schema,name:tables[name],ref=lambda name:tables[name])
        root = Path(__file__).resolve().parents[1]
        query = env.from_string((root/'dbt_project/tests/singular/minimal_batch_partition.sql').read_text(encoding='utf-8')).render()
        with closing(psycopg2.connect(host=os.getenv('DBT_HOST','127.0.0.1'),
                     port=os.getenv('DBT_PORT','5433'),dbname=database,user=os.environ['POSTGRES_USER'],
                     password=os.getenv('POSTGRES_PASSWORD',''))) as con, con:
            with con.cursor() as cursor:
                for n in names:
                    cursor.execute('create temporary table p_'+n+'(source_record_key text,batch_id bigint,file_version_id bigint,primary_disposition text) on commit drop')
                for label, rows, expected in cases:
                    with self.subTest(label=label):
                        for entry in rows:
                            cursor.execute('insert into p_'+entry[0]+' values(%s,%s,%s,%s)',entry[1:])
                        cursor.execute(query)
                        self.assertEqual(sorted(cursor.fetchall()),sorted(expected))
                        for n in names:cursor.execute('truncate p_'+n)


if __name__ == '__main__':
    unittest.main()
