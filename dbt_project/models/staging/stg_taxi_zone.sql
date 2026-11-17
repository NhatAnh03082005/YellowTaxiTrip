-- CSV names are documented; jsonb projection tolerates quoted or lowercase loader names.
with source_rows as (
    select to_jsonb(z) as raw_zone
    from {{ source('stage', 'taxi_zone_raw') }} z
)
select
    coalesce(raw_zone ->> 'LocationID', raw_zone ->> 'location_id')::smallint as location_id,
    coalesce(raw_zone ->> 'Borough', raw_zone ->> 'borough') as borough,
    coalesce(raw_zone ->> 'Zone', raw_zone ->> 'zone') as zone_name,
    coalesce(raw_zone ->> 'service_zone', raw_zone ->> 'ServiceZone') as service_zone
from source_rows
