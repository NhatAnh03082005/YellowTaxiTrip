with members as (
    select
        m.zone_key,
        z.location_id,
        z.borough,
        z.zone_name,
        z.service_zone,
        false as is_technical_member,
        'BUSINESS'::text as member_type
    from {{ ref('zone_key_map') }} m
    join {{ ref('stg_taxi_zone') }} z on z.location_id = m.location_id
)
select * from members
union all
select 0, null::smallint, 'Unknown', 'Unknown or unmatched zone', null::text,
       true, 'UNKNOWN'
union all
select -1, null::smallint, 'Not Applicable', 'Zone not applicable', null::text,
       true, 'NOT_APPLICABLE'
