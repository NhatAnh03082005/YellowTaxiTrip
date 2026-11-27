{{ config(tags=['minimal_batch']) }}
with violations as (
    select source_record_key
    from {{ ref('v_fact_taxi_trip_analysis') if var('dbt_minimal_mode', false) else ref('fact_taxi_trip') }}
    where
    {% if var('dbt_minimal_mode', false) %}
      file_version_id={{ minimal_version_id() }} and
    {% endif %}
      ((pickup_datetime is not null
        and to_char(pickup_datetime, 'YYYY-MM') <> source_year_month
        and not ('TIME-005'=any(dq_rule_ids)))
       or (source_year_month < '{{ var('cbd_schema_start_month') }}'
        and cbd_congestion_fee is not null))
)
select 'FILE_MONTH_CBD' as rule,count(*) as failures
from violations having count(*)>0
