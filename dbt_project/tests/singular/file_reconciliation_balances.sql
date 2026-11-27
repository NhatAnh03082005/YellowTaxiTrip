{% if var('dbt_minimal_mode', false) %}
{{ config(tags=['minimal_batch']) }}
with current_reconciliation as (
    select * from {{ ref('file_reconciliation') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
), checks as (
    select 'MISSING_RECONCILIATION'::text as rule, 1::bigint as failures
    where not exists(select 1 from current_reconciliation)
    union all
    select 'ROW_SOURCE', count(*) from current_reconciliation where source_row_delta<>0
    union all
    select 'ROW_TRUSTED', count(*) from current_reconciliation where trusted_row_delta<>0
    {% for field in ['fare','tip','total'] %}
    union all
    select '{{ field | upper }}_SOURCE_ELIGIBLE', count(*) from current_reconciliation
    where source_{{ field }}_eligible_delta<>0
    union all
    select '{{ field | upper }}_TRUSTED_ELIGIBLE', count(*) from current_reconciliation
    where trusted_{{ field }}_eligible_delta<>0
    union all
    select '{{ field | upper }}_SOURCE_AMOUNT', count(*) from current_reconciliation
    where abs(source_{{ field }}_delta) > {{ var('amount_reconciliation_tolerance') }}
    union all
    select '{{ field | upper }}_TRUSTED_AMOUNT', count(*) from current_reconciliation
    where abs(trusted_{{ field }}_delta) > {{ var('amount_reconciliation_tolerance') }}
    {% endfor %}
    union all
    select 'REVENUE_ELIGIBLE', count(*) from current_reconciliation
    where revenue_amount_rows<>revenue_eligible_rows
)
select rule, failures from checks where failures>0
{% else %}
select *
from {{ ref('file_reconciliation') }}
where source_file_sha256 is null
   or source_file is null
   or source_row_delta <> 0
   or trusted_row_delta <> 0
   or source_amount_eligible_row_delta <> 0
   or trusted_amount_eligible_row_delta <> 0
   or abs(source_amount_delta) > {{ var('amount_reconciliation_tolerance') }}
   or abs(trusted_amount_delta) > {{ var('amount_reconciliation_tolerance') }}
{% endif %}
