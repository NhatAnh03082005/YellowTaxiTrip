with minutes as (
    select generate_series(0, 1439) as minute_of_day
), members as (
    select
        (minute_of_day + 1)::smallint as time_key,
        make_time(minute_of_day / 60, minute_of_day % 60, 0) as time_of_day,
        (minute_of_day / 60)::smallint as hour_of_day,
        (minute_of_day % 60)::smallint as minute_of_hour,
        minute_of_day::smallint as minute_of_day,
        case
            when minute_of_day / 60 >= {{ var('time_band_start_hours')[0] }}
                and minute_of_day / 60 < {{ var('time_band_start_hours')[1] }} then 'MORNING'
            when minute_of_day / 60 >= {{ var('time_band_start_hours')[1] }}
                and minute_of_day / 60 < {{ var('time_band_start_hours')[2] }} then 'AFTERNOON'
            when minute_of_day / 60 >= {{ var('time_band_start_hours')[2] }}
                and minute_of_day / 60 < {{ var('time_band_start_hours')[3] }} then 'EVENING'
            else 'NIGHT'
        end as time_band,
        false as is_technical_member,
        'BUSINESS'::text as member_type
    from minutes
)
select * from members
union all
select 0, null::time, null::smallint, null::smallint, null::smallint,
       'UNKNOWN',
       true, 'UNKNOWN'
union all
select -1, null::time, null::smallint, null::smallint, null::smallint,
       'NOT_APPLICABLE',
       true, 'NOT_APPLICABLE'
