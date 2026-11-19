{% if var('dbt_minimal_mode', false) %}
{{ config(materialized='minimal_reference',tags=['minimal_reference'], reference_key='rate_code_key', reference_natural_key='rate_code_id',reference_dependencies=['rate_codes']) }}
{% endif %}
select rate_code_key, rate_code_id, rate_code_name,
       false as is_technical_member, 'BUSINESS'::text as member_type
from {{ ref('rate_codes') }}
union all
select 0, null::smallint, 'Unknown or unmapped rate', true, 'UNKNOWN'
union all
select -1, null::smallint, 'Rate not applicable', true, 'NOT_APPLICABLE'
