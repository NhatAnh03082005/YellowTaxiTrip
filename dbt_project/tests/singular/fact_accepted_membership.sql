with accepted_for_load as (
    select source_record_key
    from {{ ref('int_taxi_trip_accepted') }}
    {% if var('load_batch_id', none) is not none %}
    where batch_id = {{ var('load_batch_id') | int }}
    {% endif %}
)
select a.source_record_key
from accepted_for_load a
left join {{ ref('fact_taxi_trip') }} f on f.source_record_key = a.source_record_key
where f.source_record_key is null
union all
select f.source_record_key
from {{ ref('fact_taxi_trip') }} f
left join {{ ref('int_taxi_trip_accepted') }} a on a.source_record_key = f.source_record_key
where f.file_version_id in (
       select distinct file_version_id from {{ ref('int_taxi_trip_classified') }}
   )
  and (a.source_record_key is null
   or f.trip_count is distinct from 1::smallint
   or f.raw_exact_fingerprint is distinct from a.raw_exact_fingerprint
   or f.candidate_trip_fingerprint is distinct from a.candidate_trip_fingerprint
   or row(f.pickup_datetime, f.dropoff_datetime, f.vendor_id, f.rate_code_id,
          f.payment_type_id, f.pu_location_id, f.do_location_id,
          f.passenger_count, f.trip_distance_miles, f.duration_seconds,
          f.row_speed_mph)
      is distinct from
      row(a.pickup_datetime, a.dropoff_datetime, a.vendor_id, a.rate_code_id,
          a.payment_type_id, a.pu_location_id, a.do_location_id,
          a.passenger_count, a.trip_distance, a.duration_seconds,
          a.row_speed_mph)
   or row(f.fare_amount, f.extra, f.mta_tax, f.tip_amount, f.tolls_amount,
          f.improvement_surcharge, f.total_amount, f.congestion_surcharge,
          f.airport_fee, f.cbd_congestion_fee, f.reported_total_amount,
          f.calculated_total_amount, f.row_tip_rate)
      is distinct from
      row(a.fare_amount, a.extra, a.mta_tax, a.tip_amount, a.tolls_amount,
          a.improvement_surcharge, a.total_amount, a.congestion_surcharge,
          a.airport_fee, a.cbd_congestion_fee, a.reported_total_amount,
          a.calculated_total_amount, a.row_tip_rate)
   or f.primary_disposition is distinct from a.primary_disposition
   or f.dq_rule_ids is distinct from a.dq_rule_ids
   or f.revenue_amount is distinct from a.revenue_amount
   or f.is_revenue_kpi_eligible is distinct from a.is_revenue_kpi_eligible
   or f.is_tip_kpi_eligible is distinct from a.is_tip_kpi_eligible
   or f.is_speed_kpi_eligible is distinct from a.is_speed_kpi_eligible
   or f.is_duration_kpi_eligible is distinct from a.is_duration_kpi_eligible
   or f.is_passenger_kpi_eligible is distinct from a.is_passenger_kpi_eligible
   or row(f.batch_id, f.source_file, f.source_year_month,
          f.source_file_sha256, f.source_row_number, f.schema_fingerprint,
          f.schema_version, f.source_last_modified, f.ingested_at)
      is distinct from
      row(a.batch_id, a.source_file, a.source_year_month,
          a.source_file_sha256, a.source_row_number, a.schema_fingerprint,
          a.schema_version, a.source_last_modified, a.ingested_at)
   or exists (select 1 from {{ ref('business_quarantine') }} b where b.source_record_key = f.source_record_key)
   or exists (select 1 from {{ ref('duplicate_quarantine') }} d where d.source_record_key = f.source_record_key))
