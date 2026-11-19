{{ config(enabled=(not var('dbt_minimal_mode', false)) and var('fixture_mode', false)) }}

with actual as (
    select source_row_number, dq_rule_ids, primary_disposition
    from {{ ref('int_taxi_trip_classified') }}
    where source_file = '{{ var('business_fixture_source_file') }}'
), expected as (
    select fixture_row_number, case_id, expected_rule_id
    from {{ ref('business_fixture_expectations') }}
)
select e.case_id
from expected e
left join actual a on a.source_row_number = e.fixture_row_number
where a.source_row_number is null
   or (e.expected_rule_id <> 'NONE'
       and not e.expected_rule_id = any(a.dq_rule_ids))
   or (e.expected_rule_id = 'DUP-001' and a.primary_disposition <> 'DUPLICATE')
union all
select 'UNEXPECTED_ROW'
from actual a
left join expected e on e.fixture_row_number = a.source_row_number
where e.fixture_row_number is null
