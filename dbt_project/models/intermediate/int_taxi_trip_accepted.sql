select *
from {{ ref('int_taxi_trip_classified') }}
where primary_disposition in ('ACCEPTED', 'WARNING')
