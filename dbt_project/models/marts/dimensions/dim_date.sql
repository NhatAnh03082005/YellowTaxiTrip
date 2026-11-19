{% if var('dbt_minimal_mode', false) %}
{{ config(materialized='minimal_reference',tags=['minimal_reference'], reference_key='date_key', reference_natural_key='full_date',reference_dependencies=[]) }}
{% endif %}
{% if var('dbt_minimal_mode', false) %}
with bounds as (select '{{ var("minimal_calendar_start") }}'::date as first_date, '{{ var("minimal_calendar_end") }}'::date as last_date
{% else %}
{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='date_key') }}

with observed_dates as (
    select pickup_datetime::date as full_date from {{ ref('int_taxi_trip_accepted') }}
    union all
    select dropoff_datetime::date from {{ ref('int_taxi_trip_accepted') }}
    {% if is_incremental() %}
    union all
    select full_date from {{ this }}
    {% endif %}
), bounds as (
    select
        min(full_date) as first_date,
        max(full_date) as last_date
    from observed_dates

{% endif %}
), calendar as (
    select day_value::date as full_date
    from bounds,
         generate_series(first_date, last_date, interval '1 day') as day_value
), members as (
    select
        (extract(year from full_date)::integer * 10000
         + extract(month from full_date)::integer * 100
         + extract(day from full_date)::integer) as date_key,
        full_date,
        extract(year from full_date)::smallint as calendar_year,
        extract(quarter from full_date)::smallint as calendar_quarter,
        extract(month from full_date)::smallint as calendar_month,
        extract(day from full_date)::smallint as day_of_month,
        extract(isodow from full_date)::smallint as iso_day_of_week,
        false as is_technical_member,
        'BUSINESS'::text as member_type
    from calendar
)
select * from members
union all
select 0, null::date, null::smallint, null::smallint, null::smallint,
       null::smallint, null::smallint, true, 'UNKNOWN'
union all
select -1, null::date, null::smallint, null::smallint, null::smallint,
       null::smallint, null::smallint, true, 'NOT_APPLICABLE'
