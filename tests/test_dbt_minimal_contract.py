"""Run offline: python -m unittest discover -s tests -p test_dbt_minimal_contract.py -v.

This validates fixture/oracle consistency, not future dbt/runner implementation.
"""
import copy
import json
from decimal import Decimal
from pathlib import Path
import unittest

from dbt_minimal_oracle import classify_file, raw_hash, source_key, summarize, RAW_FIELDS

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    return json.loads((ROOT / path).read_text(encoding='utf-8'))


def compare_actual_rows(actual, expected):
    """Reusable assertion for a future SQL output adapter. Never sample rows."""
    ids = [r['case_id'] for r in actual]
    if len(ids) != len(set(ids)) or set(ids) != set(expected):
        raise AssertionError('Missing, extra or duplicate case_id in actual output')
    for row in actual:
        for field, wanted in expected[row['case_id']].items():
            if row.get(field) != wanted:
                raise AssertionError(f'{row["case_id"]}.{field}: {row.get(field)!r} != {wanted!r}')


class MinimalContractFixtures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input = load('tests/fixtures/dbt_minimal/input.json')
        cls.expected = load('tests/expected_results/dbt_minimal_rows.json')
        cls.summaries = load('tests/expected_results/dbt_minimal_summaries.json')
        cls.rows = {f['id']: classify_file(f, set(cls.input['known_zones'])) for f in cls.input['files']}

    def test_all_literal_rows_and_eligibility(self):
        compare_actual_rows([r for rows in self.rows.values() for r in rows], self.expected)

    def test_literal_counts_money_and_kpi_sufficient_statistics(self):
        for scope, rows in self.rows.items():
            with self.subTest(scope=scope):
                self.assertEqual(summarize(rows), self.summaries[scope])

    def test_branch_reconciliation_with_invalid_amount_counts(self):
        for scope, rows in self.rows.items():
            s=summarize(rows)
            self.assertEqual(s['raw_rows'], s['canonical_rows']+s['technical_rows'])
            self.assertEqual(s['canonical_rows'],s['fact_rows']+s['business_rows']+s['duplicate_rows'])
            for field, amounts in s['measures'].items():
                branches=[amounts[k] for k in ('TECHNICAL_REJECT','TRUSTED','BUSINESS_QUARANTINE','DUPLICATE')]
                self.assertEqual(amounts['ALL']['eligible_count'],sum(v['eligible_count'] for v in branches))
                self.assertEqual(Decimal(amounts['ALL']['sum']),sum(Decimal(v['sum']) for v in branches))
        self.assertEqual(self.summaries['A_2024']['measures']['total']['ALL']['eligible_count'],27)

    def test_exact_survivor_and_precision_distinction(self):
        rows={r['case_id']:r for r in self.rows['A_2024']}
        control=rows['A01_CONTROL']; loser=rows['A02_EXACT_LOSER']
        self.assertEqual(loser['survivor_source_record_key'],control['source_record_key'])
        self.assertEqual(loser['duplicate_occurrence_number'],2)
        for case in ('A03_CANDIDATE_ONLY','A04_RAW_PRECISION_DISTINCT'):
            self.assertNotEqual(rows[case]['raw_hash'],control['raw_hash'])
            self.assertEqual(rows[case]['candidate'],control['candidate'])
            self.assertIn(rows[case]['disposition'],('ACCEPTED','WARNING'))

    def test_cross_file_match_does_not_rewrite_previous_rows(self):
        before=copy.deepcopy(self.rows['A_2024'])
        current=self.rows['C_CROSS_FILE'][0]
        self.assertEqual(current['raw_hash'],before[0]['raw_hash'])
        self.assertNotEqual(current['source_record_key'],before[0]['source_record_key'])
        self.assertNotIn('DUP-002',current['rules'])
        self.assertEqual(current['disposition'],'WARNING')  # TIME-005 only
        self.assertEqual(before,self.rows['A_2024'])

    def test_shared_day_and_replacement_expected_grains(self):
        def daily(scopes):
            totals={}
            for scope in scopes:
                file=next(f for f in self.input['files'] if f['id']==scope)
                raw={r['case_id']:r['raw'] for r in file['rows']}
                for row in self.rows[scope]:
                    if row['revenue'] is not None:
                        day=raw[row['case_id']]['tpep_pickup_datetime'][:10]
                        count,amount=totals.get(day,(0,Decimal(0)))
                        totals[day]=(count+1,amount+row['revenue'])
            return totals
        before=daily(['A_2024','B_2025','C_CROSS_FILE'])
        after=daily(['A_REPLACEMENT','B_2025','C_CROSS_FILE'])
        self.assertEqual(before['2024-05-01'],(19,Decimal('248.03')))
        self.assertEqual(after['2024-05-01'],(2,Decimal('26.00')))
        self.assertEqual(after['2024-05-02'],(1,Decimal('13.00')))
        self.assertEqual(sum(v[0] for v in before.values()),23)
        self.assertEqual(sum(v[1] for v in before.values()),Decimal('320.03'))
        self.assertEqual(sum(v[0] for v in after.values()),7)
        self.assertEqual(sum(v[1] for v in after.values()),Decimal('111.00'))

    def test_ratio_of_sums_is_not_average_of_averages(self):
        rows=[r for r in self.rows['B_2025'] if r['eligible']['tip']]
        correct=sum(r['tip'] for r in rows)/sum(r['fare'] for r in rows)
        wrong=sum(r['tip']/r['fare'] for r in rows)/len(rows)
        self.assertEqual(correct,Decimal(9)/55)
        self.assertNotEqual(correct,wrong)
        speed_rows=[r for r in self.rows['B_2025'] if r['eligible']['speed']]
        self.assertNotEqual(sum(r['distance'] for r in speed_rows)*3600/sum(r['duration_seconds'] for r in speed_rows),
                            sum(r['speed'] for r in speed_rows)/len(speed_rows))

    def test_legacy_raw_golden_vectors_independent_of_sql(self):
        values=['1','2024-05-01 01:02:03','2024-05-01 01:12:03','1','1.25','1','N','132','138','1',
                '8.0','0.5','0.5','1.5','0','0.3','10.8','2.5','0']
        raw=dict(zip((k for k,_ in RAW_FIELDS),values))
        self.assertEqual(raw_hash(raw,'1'*64,False),'85578a876b06f4f020df08f495592a737c6c3c47ef6859a29bba822747e5642d')
        raw.update(tpep_pickup_datetime='2025-05-01 01:02:03',tpep_dropoff_datetime='2025-05-01 01:12:03',cbd_congestion_fee='2.75')
        self.assertEqual(raw_hash(raw,'2'*64,True),'6cc97b0cd280e63eb18fe0002ef5ba2f0e3bcf8011ad09b236c4b215d15b877d')
        self.assertEqual(source_key('A'*64,1),self.expected['A01_CONTROL']['source_record_key'])

    def test_fixture_schema_shape(self):
        for file in self.input['files']:
            for row in file['rows']:
                self.assertEqual('cbd_congestion_fee' in row['raw'],file['has_cbd'])
                self.assertTrue(all(k in row['raw'] for k,_ in RAW_FIELDS))
        self.assertEqual(len(self.rows['EMPTY_VALIDATED_SCOPE']),0)
        self.assertEqual(self.rows['ALL_REJECTED'][0]['disposition'],'TECHNICAL_REJECT')

    def test_negative_actual_outputs_are_rejected(self):
        good=[r for rows in self.rows.values() for r in rows]
        mutations=[]
        changed=copy.deepcopy(good); changed[0]['eligible']['speed']=False; mutations.append(changed)
        changed=copy.deepcopy(good); changed[0]['rules']=['DUP-002']; mutations.append(changed)
        changed=copy.deepcopy(good); changed[0]['source_record_key']='wrong'; mutations.append(changed)
        mutations.extend([good[1:],good+[good[0]]])
        for actual in mutations:
            with self.assertRaises(AssertionError): compare_actual_rows(actual,self.expected)

    def test_bad_fixture_input_contract_is_rejected(self):
        cases=[]
        file=copy.deepcopy(self.input['files'][0]); del file['rows'][0]['raw']['extra']; cases.append(file)
        file=copy.deepcopy(self.input['files'][0]); file['rows'][0]['source_row_number']=0; cases.append(file)
        file=copy.deepcopy(self.input['files'][0]); file['rows'][1]['source_row_number']=1; cases.append(file)
        file=copy.deepcopy(self.input['files'][0]); file['schema_version']='unknown'; cases.append(file)
        for file in cases:
            with self.assertRaises(ValueError): classify_file(file,set(self.input['known_zones']))


if __name__ == '__main__':
    unittest.main()
