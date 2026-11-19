"""Exercise the actual candidate view SQL in PostgreSQL temporary tables."""
import subprocess
import unittest
from pathlib import Path


class FingerprintCandidateSQL(unittest.TestCase):
    def test_candidate_unions_three_branches_without_deduplication(self):
        root = Path(__file__).resolve().parents[1] / 'dbt_project'
        renderer = (
            'import sys,jinja2; e=jinja2.Environment(); '
            'e.globals.update(config=lambda **k:"",var=lambda k,d=None:1 if k=="fingerprint_parity_file_version" else d,'
            'source=lambda s,t:"pg_temp."+t,ref=lambda t:"pg_temp."+t); '
            'print(e.from_string(sys.stdin.read()).render())'
        )
        def render(path):
            return subprocess.run(['docker', 'exec', '-i', 'nyc_taxi_bi-mage-1', 'python', '-c', renderer],
                                  input=(root / path).read_text(encoding='utf-8'),
                                  text=True, capture_output=True, check=True).stdout

        sql = render('models/audit/v_trip_fingerprint_history.sql')
        warning = render('models/audit/v_trip_cross_file_warning.sql')
        parity = render('tests/singular/fingerprint_history_candidate_parity.sql')
        setup = '''
begin;
create temp table fact_taxi_trip (source_record_key text, file_version_id bigint, source_row_number bigint, source_file_sha256 text, raw_exact_fingerprint text, candidate_trip_fingerprint text, primary_disposition text, batch_id bigint);
create temp table business_quarantine (like fact_taxi_trip);
create temp table duplicate_quarantine (like fact_taxi_trip);
create temp table history (source_record_key text, from_disposition text, change_type text, event_id bigint, prior_fact jsonb);
insert into fact_taxi_trip values (repeat('a',64), 1, 1, repeat('b',64), repeat('c',64), repeat('d',64), 'ACCEPTED', 1);
insert into business_quarantine values (repeat('e',64), 1, 2, repeat('b',64), repeat('c',64), repeat('d',64), 'BUSINESS_QUARANTINE', 1);
insert into duplicate_quarantine values (repeat('f',64), 1, 3, repeat('b',64), repeat('c',64), repeat('d',64), 'DUPLICATE', 1);
create temp view v_trip_fingerprint_history as
'''
        assertions = ''';
create temp view candidate as select * from v_trip_fingerprint_history;
create temp table trip_fingerprint_history as select * from candidate;
create temp table cross_file_fingerprint_match(fingerprint_type text, fingerprint bytea, first_file_version_id bigint, matched_file_version_id bigint);
insert into fact_taxi_trip values (repeat('9',64), 2, 1, repeat('8',64), repeat('c',64), repeat('d',64), 'WARNING', 2);
insert into cross_file_fingerprint_match values
    ('RAW_EXACT', decode(repeat('c',64),'hex'),1,2),
    ('CANDIDATE', decode(repeat('d',64),'hex'),1,2);
insert into history select source_record_key, primary_disposition, 'REPLACE_VERSION', 1, to_jsonb(f)
from fact_taxi_trip f where file_version_id = 1;
delete from fact_taxi_trip where file_version_id = 1;
create temp view warning as
''' + warning + ''';
create temp view parity as
''' + parity + ''';
do $$ begin
  if (select count(*) from candidate) <> 4 or
     (select count(distinct source_record_key) from candidate) <> 4 or
     (select count(*) from warning where dq_rule_id = 'DUP-002') <> 4 or
     exists (select from parity)
  then raise exception 'candidate fixture mismatch'; end if;
end $$;
update trip_fingerprint_history set source_row_number = 99 where source_row_number = 1;
do $$ begin
  if (select count(*) from parity) <> 2 then raise exception 'parity failed to detect difference'; end if;
end $$;
rollback;
'''
        result = subprocess.run(['docker', 'exec', '-i', 'nyc_taxi_bi-postgres-1', 'sh', '-c',
                                 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
                                input=setup + sql + assertions, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
