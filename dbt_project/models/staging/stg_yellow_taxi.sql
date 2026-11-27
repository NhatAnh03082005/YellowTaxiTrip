-- Canonical types and lineage are owned by Mage. Keep local New York wall time.
with canonical as (
    select
        s.*,
        {% if var('dbt_minimal_mode', false) %}
        {# The scope gate checks non-null version and all registry metadata.
           Minimal staging is a thin canonical projection; no redundant join. #}
        s.file_version_id as effective_file_version_id
        {% else %}
        coalesce(s.file_version_id, v.file_version_id) as effective_file_version_id
        {% endif %}
    from {{ source('stage', 'yellow_taxi_canonical') }} s
    {% if not var('dbt_minimal_mode', false) %}
    join {{ source('control', 'source_file_version') }} v
      on v.source_file = s.source_file
     and v.source_file_sha256 = s.source_file_sha256
    {% endif %}
)
select
    stage_row_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag as source_store_and_fwd_flag,
    case when upper(trim(store_and_fwd_flag)) in ('Y', 'N')
        then upper(trim(store_and_fwd_flag)) end::char(1) as store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    cast_error_flags,
    {% if not var('dbt_minimal_mode', false) %}raw_payload,{% endif %}
    batch_id,
    effective_file_version_id as file_version_id,
    source_file,
    source_year_month,
    source_file_sha256,
    source_row_number,
    source_record_key,
    schema_fingerprint,
    schema_version,
    source_last_modified,
    ingested_at
from canonical

{% if var('dbt_minimal_mode', false) %}
where batch_id = {{ minimal_batch_id() }} and effective_file_version_id = {{ minimal_version_id() }}
{% endif %}
