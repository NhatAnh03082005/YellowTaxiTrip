{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='date_key') }}

with accepted_bounds as (
    select
        min(least(pickup_datetime::date, dropoff_datetime::date)) as first_date,
        max(greatest(pickup_datetime::date, dropoff_datetime::date)) as last_date
    from {{ ref('int_taxi_trip_accepted') }}
), bounds as (
    select
        {% if is_incremental() %}
        least(a.first_date, (select min(full_date) from {{ this }})) as first_date,
        greatest(a.last_date, (select max(full_date) from {{ this }})) as last_date
        {% else %}
        a.first_date,
        a.last_date
        {% endif %}
    from accepted_bounds a
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
