select f.source_record_key
from {{ ref('fact_taxi_trip') }} f
left join {{ ref('dim_date') }} pd on pd.date_key = f.pickup_date_key
left join {{ ref('dim_date') }} dd on dd.date_key = f.dropoff_date_key
left join {{ ref('dim_time') }} pt on pt.time_key = f.pickup_time_key
left join {{ ref('dim_time') }} dt on dt.time_key = f.dropoff_time_key
where pd.full_date is distinct from f.pickup_datetime::date
   or dd.full_date is distinct from f.dropoff_datetime::date
   or pt.hour_of_day is distinct from extract(hour from f.pickup_datetime)::smallint
   or pt.minute_of_hour is distinct from extract(minute from f.pickup_datetime)::smallint
   or dt.hour_of_day is distinct from extract(hour from f.dropoff_datetime)::smallint
   or dt.minute_of_hour is distinct from extract(minute from f.dropoff_datetime)::smallint
