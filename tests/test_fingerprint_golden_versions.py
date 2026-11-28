"""Golden v001/v002 fingerprint vectors executed through the dbt model SQL."""
from pathlib import Path
import copy
import json
import subprocess
from types import SimpleNamespace
import unittest

import jinja2


class FingerprintGoldenVersions(unittest.TestCase):
    def test_minimal_native_projection_preserves_hash_and_contract(self):
        root = Path(__file__).resolve().parents[1]
        dbt = root / 'dbt_project'
        fields = {
            'VendorID': 'integer', 'tpep_pickup_datetime': 'timestamp',
            'tpep_dropoff_datetime': 'timestamp', 'passenger_count': 'bigint',
            'trip_distance': 'double precision', 'RatecodeID': 'bigint',
            'store_and_fwd_flag': 'text', 'PULocationID': 'integer',
            'DOLocationID': 'integer', 'payment_type': 'bigint',
            **{name: 'double precision' for name in (
                'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount', 'congestion_surcharge',
                'Airport_fee', 'airport_fee', 'cbd_congestion_fee')},
            'schema_version': 'text', 'raw_payload': 'jsonb', 'source_file': 'text',
            'case_id': 'integer',
        }
        base = dict(zip(list(fields)[:10], (
            1, '2024-05-01 01:02:03.123456', '2024-05-01 01:12:03.654321',
            1, 1.250000000000001, 1, 'N', 132, 138, 1)))
        base.update({name: 0.5 for name in list(fields)[10:21]})
        base.update(raw_payload=None, source_file='metadata ' * 100)
        cases = []
        for version in ('yt_schema_v001', 'yt_schema_v002'):
            original = dict(base, schema_version=version,
                            cbd_congestion_fee=None if version.endswith('001') else 2.75)
            variants = [original]
            for name in list(fields)[:21]:
                variants.append(dict(original, **{name: None}))
            variants.extend(dict(original, trip_distance=value)
                            for value in ('NaN', 'Infinity', '-Infinity', -0.0))
            variants.append(dict(original, store_and_fwd_flag='é漢字'))
            payload = {k: v for k, v in original.items() if k not in
                       ('schema_version', 'raw_payload', 'source_file')}
            if version.endswith('001'):
                payload.pop('cbd_congestion_fee')
            variants.append(dict(original, raw_payload=payload))
            for value in ({}, [], 'invalid', 42):
                variants.append(dict(original, raw_payload=value))
            missing = copy.deepcopy(payload)
            missing.pop('VendorID')
            variants.append(dict(original, raw_payload=missing))
            cases.extend(variants)
        for index, case in enumerate(cases):
            case['case_id'] = index

        env = jinja2.Environment(extensions=['jinja2.ext.do'])
        scalar = env.from_string((dbt / 'macros/fingerprint_fields.sql').read_text(
            encoding='utf-8')).make_module()
        env.globals.update(execute=True,
                           var=lambda name, default=None: True if name == 'dbt_minimal_mode' else default,
                           fingerprint_field=scalar.fingerprint_field,
                           raw_source_value=scalar.raw_source_value)
        helpers = env.from_string((dbt / 'macros/minimal_raw_fingerprints.sql').read_text(
            encoding='utf-8')).make_module()
        env.globals['minimal_raw_contract_valid'] = helpers.minimal_raw_contract_valid
        checks = []
        shapes = [set(fields), set(fields) - {'cbd_congestion_fee'},
                  set(fields) - {'raw_payload'}, set(fields) - {'VendorID'},
                  set(fields) - {'Airport_fee'}, set(fields) - {'airport_fee'},
                  set(fields) - {'raw_payload', 'cbd_congestion_fee'},
                  {'schema_version', 'case_id', 'source_file'},
                  {'schema_version', 'case_id', 'source_file', 'raw_payload'}]
        for columns in shapes:
            adapter = SimpleNamespace(
                get_columns_in_relation=lambda relation, cols=columns:
                    [SimpleNamespace(name=name) for name in cols],
                quote=lambda name: '"' + name + '"')
            env.globals['adapter'] = adapter
            actual = env.from_string((dbt / 'macros/minimal_raw_values.sql').read_text(
                encoding='utf-8')).make_module().minimal_raw_values('unused', 'r')
            # Independent pre-optimization whole-record fallback; do not derive
            # the reference from the candidate's projected field list.
            branches = []
            if 'raw_payload' in columns:
                branches.append("when jsonb_typeof(to_jsonb(r.raw_payload))='object' then to_jsonb(r.raw_payload)")
            if 'cbd_congestion_fee' in columns:
                branches.append("when r.schema_version='yt_schema_v001' and coalesce(to_jsonb(r.cbd_congestion_fee),'null'::jsonb)='null'::jsonb then to_jsonb(r)-'cbd_congestion_fee'")
            expected = ('case ' + ' '.join(branches) + ' else to_jsonb(r) end'
                        if branches else 'to_jsonb(r)')
            projection = ','.join('"' + name + '"' for name in fields if name in columns)
            def result(expression):
                return ("select case_id, " + str(helpers.minimal_raw_fingerprint()) + " as hash, "
                        + str(helpers.minimal_raw_contract_valid()) + " as valid from ("
                        "select r.case_id,repeat('1',64)::text schema_fingerprint,r.schema_version,"
                        + str(expression) + " raw_values from (select " + projection
                        + " from raw_native_cases) r) q")
            checks.append('(('+result(expected)+') except all ('+result(actual)+'))')
            checks.append('(('+result(actual)+') except all ('+result(expected)+'))')
            direct = env.from_string((dbt / 'macros/minimal_raw_scalar_input.sql').read_text(
                encoding='utf-8')).make_module()
            source = ("select r.case_id,repeat('1',64)::text schema_fingerprint,r.schema_version,"
                      + str(direct.minimal_raw_scalar_source('unused', 'r'))
                      + ' from (select ' + projection + ' from raw_native_cases) r')
            raw_fields = ('select case_id,schema_fingerprint,schema_version,true as raw_values_present,'
                          + str(direct.minimal_raw_scalar_fields()) + ' from (' + source + ') s')
            scalar_result = ('select case_id,' + str(helpers.minimal_raw_fingerprint(True))
                             + ' as hash,raw_contract_valid as valid from (' + raw_fields + ') q')
            checks.append('(('+result(expected)+') except all ('+scalar_result+'))')
            checks.append('(('+scalar_result+') except all ('+result(expected)+'))')
        definitions = ','.join('"'+name+'" '+kind for name, kind in fields.items())
        payload_json = json.dumps(cases, ensure_ascii=False).replace("'", "''")
        sql = ('begin; create temp table raw_native_shape(' + definitions + '); '
               "create temp table raw_native_cases as select r.* from jsonb_array_elements('"
               + payload_json + "'::jsonb) p cross join lateral jsonb_populate_record("
               'null::raw_native_shape,p.value) r; do $$ begin if exists('
               + ' union all '.join(checks)
               + ") then raise exception 'Native raw projection changed hash/contract'; end if; end $$; rollback;")
        result = subprocess.run(
            ['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
            cwd=root, input=sql, text=True, capture_output=True, encoding='utf-8')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_2024_and_2025_hashes_are_stable(self):
        root = Path(__file__).resolve().parents[1]
        dbt = root / 'dbt_project'
        env = jinja2.Environment()
        macros = env.from_string((dbt / 'macros/fingerprint_fields.sql').read_text(encoding='utf-8')).make_module()
        env.globals.update(
            config=lambda **kwargs: '',
            source=lambda schema, table: 'pg_temp.yellow_taxi_raw',
            ref=lambda name: 'pg_temp.enriched',
            var=lambda name, default=None: {'cbd_schema_start_month': '2025-01'}.get(name, default),
            fingerprint_field=macros.fingerprint_field,
            raw_source_value=macros.raw_source_value,
        )
        model = env.from_string(
            (dbt / 'models/intermediate/int_taxi_trip_fingerprinted.sql').read_text(encoding='utf-8')
        ).render()
        sql = r"""
BEGIN;
CREATE TEMP TABLE yellow_taxi_raw(source_record_key text, raw_payload jsonb, schema_version text);
CREATE TEMP TABLE enriched(
  source_record_key text, schema_fingerprint text, source_year_month text,
  vendor_id smallint, pickup_datetime timestamp, dropoff_datetime timestamp,
  pu_location_id smallint, do_location_id smallint, passenger_count smallint,
  trip_distance numeric(10,3), reported_total_amount numeric(12,2)
);
INSERT INTO yellow_taxi_raw(source_record_key, raw_payload) VALUES
('k24', '{"VendorID":"1","tpep_pickup_datetime":"2024-05-01 01:02:03","tpep_dropoff_datetime":"2024-05-01 01:12:03","passenger_count":"1","trip_distance":"1.25","RatecodeID":"1","store_and_fwd_flag":"N","PULocationID":"132","DOLocationID":"138","payment_type":"1","fare_amount":"8.0","extra":"0.5","mta_tax":"0.5","tip_amount":"1.5","tolls_amount":"0","improvement_surcharge":"0.3","total_amount":"10.8","congestion_surcharge":"2.5","Airport_fee":"0"}'),
('k25', '{"VendorID":"1","tpep_pickup_datetime":"2025-05-01 01:02:03","tpep_dropoff_datetime":"2025-05-01 01:12:03","passenger_count":"1","trip_distance":"1.25","RatecodeID":"1","store_and_fwd_flag":"N","PULocationID":"132","DOLocationID":"138","payment_type":"1","fare_amount":"8.0","extra":"0.5","mta_tax":"0.5","tip_amount":"1.5","tolls_amount":"0","improvement_surcharge":"0.3","total_amount":"10.8","congestion_surcharge":"2.5","Airport_fee":"0","cbd_congestion_fee":"2.75"}');
INSERT INTO enriched VALUES
('k24', repeat('1',64), '2024-05', 1, '2024-05-01 01:02:03', '2024-05-01 01:12:03', 132, 138, 1, 1.250, 10.80),
('k25', repeat('2',64), '2025-05', 1, '2025-05-01 01:02:03', '2025-05-01 01:12:03', 132, 138, 1, 1.250, 10.80);
CREATE TEMP VIEW actual AS
__MODEL__;
DO $$ BEGIN
  IF (SELECT trim(raw_exact_fingerprint) FROM actual WHERE source_record_key='k24') <> '85578a876b06f4f020df08f495592a737c6c3c47ef6859a29bba822747e5642d'
     OR (SELECT trim(raw_exact_fingerprint) FROM actual WHERE source_record_key='k25') <> '6cc97b0cd280e63eb18fe0002ef5ba2f0e3bcf8011ad09b236c4b215d15b877d'
     OR (SELECT trim(candidate_trip_fingerprint) FROM actual WHERE source_record_key='k24') <> '0f372fc0637a43a1fe063164b872e4b87e85c03c7e159cd475fc6ec138cf66ab'
     OR (SELECT trim(candidate_trip_fingerprint) FROM actual WHERE source_record_key='k25') <> '91c6e9257136056464477c2718f2ff72c487fd4586271dc9ee9093665a96f761'
  THEN RAISE EXCEPTION 'golden fingerprint mismatch'; END IF;
END $$;
ROLLBACK;
""".replace('__MODEL__', model)
        result = subprocess.run(
            ['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
            cwd=root, input=sql, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
