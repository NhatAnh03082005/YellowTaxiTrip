{% if var('dbt_minimal_mode', false) %}
{{ config(materialized='incremental', incremental_strategy='delete+insert',
    unique_key='file_version_id', tags=['minimal_dds', 'reconciliation', 'persistent']) }}

{# Raw supports typed source columns and lossless payload fixtures. Resolve the
   available columns once at compile time, preserving text/null fallback order. #}
{% set fields = ['fare', 'tip', 'total'] %}
{% set raw_relation = source('stage', 'yellow_taxi_raw') %}
{% set technical_relation = source('mage_dq', 'technical_quarantine') %}
with scope as (
    select f.batch_id, v.file_version_id, v.source_file_sha256, v.source_file
    from {{ source('control', 'etl_file') }} f
    join {{ source('control', 'source_file_version') }} v using (file_version_id)
    where f.batch_id = {{ minimal_batch_id() }}
      and f.file_version_id = {{ minimal_version_id() }}
      and f.file_status = 'STAGED'
      and f.source_file = v.source_file
      and f.source_file_sha256 = v.source_file_sha256
), raw_input as (
    select
    {% for field in fields %}
      {{ minimal_amount_text(raw_relation, 'r', field) }} as {{ field }}_text{{ ',' if not loop.last }}
    {% endfor %}
    from {{ source('stage', 'yellow_taxi_raw') }} r
    where r.batch_id = {{ minimal_batch_id() }} and r.file_version_id = {{ minimal_version_id() }}
), technical_input as (
    select
    {% for field in fields %}
      {{ minimal_amount_text(technical_relation, 'q', field, technical=true) }} as {{ field }}_text{{ ',' if not loop.last }}
    {% endfor %}
    from {{ source('mage_dq', 'technical_quarantine') }} q
    where q.batch_id = {{ minimal_batch_id() }} and q.file_version_id = {{ minimal_version_id() }}
), raw_values as (
    select
    {% for field in fields %}
      case when pg_input_is_valid({{ field }}_text, 'numeric(12,2)')
           then {{ field }}_text::numeric(12,2) end as {{ field }}_amount{{ ',' if not loop.last }}
    {% endfor %}
    from raw_input
), technical_values as (
    select
    {% for field in fields %}
      case when pg_input_is_valid({{ field }}_text, 'numeric(12,2)')
           then {{ field }}_text::numeric(12,2) end as {{ field }}_amount{{ ',' if not loop.last }}
    {% endfor %}
    from technical_input
), branch_rows as (
    select 'raw'::text as branch, fare_amount, tip_amount, total_amount,
           null::numeric(12,2) as revenue_amount,
           false as revenue_ok, false as tip_ok, false as speed_ok,
           false as duration_ok, false as passenger_ok
    from raw_values
    union all
    select 'canonical', fare_amount, tip_amount, total_amount,
           null::numeric(12,2), false, false, false, false, false
    from {{ source('stage', 'yellow_taxi_canonical') }}
    where batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }}
    union all
    select 'technical', fare_amount, tip_amount, total_amount,
           null::numeric(12,2), false, false, false, false, false
    from technical_values
    union all
    select 'fact', fare_amount, tip_amount, total_amount, revenue_amount,
           is_revenue_kpi_eligible, is_tip_kpi_eligible, is_speed_kpi_eligible,
           is_duration_kpi_eligible, is_passenger_kpi_eligible
    from {{ ref('fact_taxi_trip') }}
    where file_version_id={{ minimal_version_id() }}
    union all
    select 'business', fare_amount, tip_amount, total_amount,
           null::numeric(12,2), false, false, false, false, false
    from {{ ref('business_quarantine') }}
    where file_version_id={{ minimal_version_id() }}
    union all
    select 'duplicate', fare_amount, tip_amount, total_amount,
           null::numeric(12,2), false, false, false, false, false
    from {{ ref('duplicate_quarantine') }}
    where file_version_id={{ minimal_version_id() }}
), branch_agg as (
    select branch, count(*) as rows,
    {% for field in fields %}
      count({{ field }}_amount) as {{ field }}_eligible_rows,
      coalesce(sum({{ field }}_amount),0)::numeric as {{ field }}_amount,
    {% endfor %}
      count(revenue_amount) as revenue_amount_rows,
      coalesce(sum(revenue_amount),0)::numeric as revenue_amount,
      count(*) filter(where revenue_ok) as kpi_revenue_eligible_rows,
      count(*) filter(where tip_ok) as kpi_tip_eligible_rows,
      count(*) filter(where speed_ok) as kpi_speed_eligible_rows,
      count(*) filter(where duration_ok) as kpi_duration_eligible_rows,
      count(*) filter(where passenger_ok) as kpi_passenger_eligible_rows
    from branch_rows group by branch
), totals as (
    select
    {% set branches = ['raw','canonical','technical','fact','business','duplicate'] %}
    {% for branch in branches %}
      coalesce(max(rows) filter(where branch='{{ branch }}'),0) as {{ branch }}_rows,
      {% for field in fields %}
      coalesce(max({{ field }}_eligible_rows) filter(where branch='{{ branch }}'),0) as {{ branch }}_{{ field }}_eligible_rows,
      coalesce(max({{ field }}_amount) filter(where branch='{{ branch }}'),0) as {{ branch }}_{{ field }}_amount,
      {% endfor %}
    {% endfor %}
      coalesce(max(revenue_amount_rows) filter(where branch='fact'),0) as revenue_amount_rows,
      coalesce(max(revenue_amount) filter(where branch='fact'),0) as revenue_amount,
      coalesce(max(kpi_revenue_eligible_rows) filter(where branch='fact'),0) as revenue_eligible_rows,
      coalesce(max(kpi_tip_eligible_rows) filter(where branch='fact'),0) as tip_eligible_rows,
      coalesce(max(kpi_speed_eligible_rows) filter(where branch='fact'),0) as speed_eligible_rows,
      coalesce(max(kpi_duration_eligible_rows) filter(where branch='fact'),0) as duration_eligible_rows,
      coalesce(max(kpi_passenger_eligible_rows) filter(where branch='fact'),0) as passenger_eligible_rows
    from branch_agg
)
select s.*, t.*, 0::bigint as excluded_rows,
       t.raw_rows-t.canonical_rows-t.technical_rows as source_row_delta,
       t.canonical_rows-t.fact_rows-t.business_rows-t.duplicate_rows as trusted_row_delta,
       {% for field in fields %}
       t.raw_{{ field }}_eligible_rows-t.canonical_{{ field }}_eligible_rows
         -t.technical_{{ field }}_eligible_rows as source_{{ field }}_eligible_delta,
       t.canonical_{{ field }}_eligible_rows-t.fact_{{ field }}_eligible_rows
         -t.business_{{ field }}_eligible_rows-t.duplicate_{{ field }}_eligible_rows
         as trusted_{{ field }}_eligible_delta,
       t.raw_{{ field }}_amount-t.canonical_{{ field }}_amount
         -t.technical_{{ field }}_amount as source_{{ field }}_delta,
       t.canonical_{{ field }}_amount-t.fact_{{ field }}_amount
         -t.business_{{ field }}_amount-t.duplicate_{{ field }}_amount
         as trusted_{{ field }}_delta,
       {% endfor %}
       clock_timestamp() as reconciled_at
from scope s cross join totals t
{% else %}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='file_version_id',
    tags=['dq', 'reconciliation', 'persistent']
) }}

with raw_rows as (
    select r.*, coalesce(r.file_version_id, v.file_version_id) as effective_file_version_id
    from {{ source('stage', 'yellow_taxi_raw') }} r
    join {{ source('control', 'source_file_version') }} v
      on v.source_file = r.source_file
     and v.source_file_sha256 = r.source_file_sha256
), canonical_source as (
    select c.*, coalesce(c.file_version_id, v.file_version_id) as effective_file_version_id
    from {{ source('stage', 'yellow_taxi_canonical') }} c
    join {{ source('control', 'source_file_version') }} v
      on v.source_file = c.source_file
     and v.source_file_sha256 = c.source_file_sha256
), technical_source as (
    select q.*, coalesce(q.file_version_id, v.file_version_id) as effective_file_version_id
    from {{ source('mage_dq', 'technical_quarantine') }} q
    join {{ source('control', 'source_file_version') }} v
      on v.source_file = q.source_file
     and v.source_file_sha256 = q.source_file_sha256
), current_files as (
    select distinct
        effective_file_version_id as file_version_id,
        batch_id,
        source_file_sha256,
        source_file
    from raw_rows
), raw_agg as (
    select
        r.effective_file_version_id as file_version_id,
        max(r.batch_id) as batch_id,
        max(r.source_file_sha256) as source_file_sha256,
        max(r.source_file) as source_file,
        count(*) as row_count,
        count(r.total_amount) as amount_eligible_rows,
        coalesce(sum(r.total_amount::numeric(12,2)), 0)::numeric as amount_sum
    from raw_rows r
    join current_files k on k.file_version_id = r.effective_file_version_id
    group by r.effective_file_version_id
), canonical_agg as (
    select c.effective_file_version_id as file_version_id, count(*) as row_count,
           count(c.total_amount) as amount_eligible_rows,
           coalesce(sum(c.total_amount), 0)::numeric as amount_sum
    from canonical_source c
    join current_files k on k.file_version_id = c.effective_file_version_id
    group by c.effective_file_version_id
), technical_agg as (
    select q.effective_file_version_id as file_version_id, count(*) as row_count,
           count(*) filter (
               where pg_input_is_valid(q.raw_payload ->> 'total_amount', 'numeric(12,2)')
           ) as amount_eligible_rows,
           coalesce(sum(
               case when pg_input_is_valid(
                   q.raw_payload ->> 'total_amount', 'numeric(12,2)'
               ) then (q.raw_payload ->> 'total_amount')::numeric(12,2) end
           ), 0)::numeric as amount_sum
    from technical_source q
    join current_files k on k.file_version_id = q.effective_file_version_id
    group by q.effective_file_version_id
), fact_agg as (
    select f.file_version_id, count(*) as row_count,
           count(f.total_amount) as amount_eligible_rows,
           coalesce(sum(f.total_amount), 0)::numeric as amount_sum
    from {{ ref('fact_taxi_trip') }} f
    join current_files k using (file_version_id)
    group by f.file_version_id
), business_agg as (
    select b.file_version_id, count(*) as row_count,
           count(b.total_amount) as amount_eligible_rows,
           coalesce(sum(b.total_amount), 0)::numeric as amount_sum
    from {{ ref('business_quarantine') }} b
    join current_files k using (file_version_id)
    group by b.file_version_id
), duplicate_agg as (
    select d.file_version_id, count(*) as row_count,
           count(d.total_amount) as amount_eligible_rows,
           coalesce(sum(d.total_amount), 0)::numeric as amount_sum
    from {{ ref('duplicate_quarantine') }} d
    join current_files k using (file_version_id)
    group by d.file_version_id
), amounts as (
    select
        k.file_version_id,
        k.batch_id,
        k.source_file_sha256,
        k.source_file,
        coalesce(r.row_count, 0) as raw_rows,
        coalesce(c.row_count, 0) as canonical_rows,
        coalesce(t.row_count, 0) as technical_rows,
        coalesce(f.row_count, 0) as fact_rows,
        coalesce(b.row_count, 0) as business_rows,
        coalesce(d.row_count, 0) as duplicate_rows,
        coalesce(r.amount_eligible_rows, 0) as raw_amount_eligible_rows,
        coalesce(c.amount_eligible_rows, 0) as canonical_amount_eligible_rows,
        coalesce(t.amount_eligible_rows, 0) as technical_amount_eligible_rows,
        coalesce(f.amount_eligible_rows, 0) as fact_amount_eligible_rows,
        coalesce(b.amount_eligible_rows, 0) as business_amount_eligible_rows,
        coalesce(d.amount_eligible_rows, 0) as duplicate_amount_eligible_rows,
        coalesce(r.amount_sum, 0) as raw_amount,
        coalesce(c.amount_sum, 0) as canonical_amount,
        coalesce(t.amount_sum, 0) as technical_amount,
        coalesce(f.amount_sum, 0) as fact_amount,
        coalesce(b.amount_sum, 0) as business_amount,
        coalesce(d.amount_sum, 0) as duplicate_amount
    from current_files k
    left join raw_agg r using (file_version_id)
    left join canonical_agg c using (file_version_id)
    left join technical_agg t using (file_version_id)
    left join fact_agg f using (file_version_id)
    left join business_agg b using (file_version_id)
    left join duplicate_agg d using (file_version_id)
)
select
    a.*,
    raw_rows - canonical_rows - technical_rows as source_row_delta,
    canonical_rows - fact_rows - business_rows - duplicate_rows as trusted_row_delta,
    raw_amount_eligible_rows - canonical_amount_eligible_rows
        - technical_amount_eligible_rows as source_amount_eligible_row_delta,
    canonical_amount_eligible_rows - fact_amount_eligible_rows
        - business_amount_eligible_rows - duplicate_amount_eligible_rows
        as trusted_amount_eligible_row_delta,
    raw_amount - canonical_amount - technical_amount as source_amount_delta,
    canonical_amount - fact_amount - business_amount - duplicate_amount
        as trusted_amount_delta,
    clock_timestamp() as reconciled_at
from amounts a
{% endif %}

