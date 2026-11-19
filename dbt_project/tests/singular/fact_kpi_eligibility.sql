{{ config(enabled=not var('dbt_minimal_mode', false)) }}
select source_record_key
from {{ ref('fact_taxi_trip') }}
where is_revenue_kpi_eligible is distinct from
      coalesce((primary_disposition in ('ACCEPTED', 'WARNING') and total_amount > 0), false)
   or is_tip_kpi_eligible is distinct from
      coalesce((primary_disposition in ('ACCEPTED', 'WARNING')
          and payment_type_id = {{ var('recorded_card_payment_code') }}
          and fare_amount > 0 and tip_amount >= 0), false)
   or is_speed_kpi_eligible is distinct from
      coalesce((primary_disposition in ('ACCEPTED', 'WARNING')
          and duration_seconds > 0
          and duration_seconds <= {{ var('duration_warning_minutes') }} * 60
          and trip_distance_miles > 0
          and row_speed_mph <= {{ var('speed_warning_mph') }}), false)
   or is_duration_kpi_eligible is distinct from
      coalesce((primary_disposition in ('ACCEPTED', 'WARNING')
          and duration_seconds > 0
          and duration_seconds <= {{ var('duration_warning_minutes') }} * 60), false)
   or is_passenger_kpi_eligible is distinct from
      coalesce((primary_disposition in ('ACCEPTED', 'WARNING')
          and passenger_count between 1 and {{ var('passenger_kpi_max') }}), false)
   or revenue_amount is distinct from
      (case when primary_disposition in ('ACCEPTED', 'WARNING') and total_amount > 0
          then total_amount end)
