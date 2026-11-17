-- Preserve the physical PostGIS column until Mage publishes its DDL/column name.
select *
from {{ source('stage', 'taxi_zone_geometry') }}
