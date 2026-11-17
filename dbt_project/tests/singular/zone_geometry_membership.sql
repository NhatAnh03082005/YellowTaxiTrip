with geometry_ids as (
    select coalesce(to_jsonb(g) ->> 'LocationID', to_jsonb(g) ->> 'location_id')::smallint as location_id
    from {{ source('stage', 'taxi_zone_geometry') }} g
), violations as (
    select location_id
    from geometry_ids
    where location_id is null
       or location_id in ({{ var('non_geometric_zone_ids') | join(', ') }})
       or location_id not in (select location_id from {{ ref('stg_taxi_zone') }})
    union all
    select null::smallint
    from geometry_ids
    having count(*) <> {{ var('zone_geometry_expected_count') }}
)
select * from violations
