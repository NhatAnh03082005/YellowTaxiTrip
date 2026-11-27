{{ config(materialized='ephemeral' if var('dbt_minimal_mode', false) else 'view') }}
{% if var('dbt_minimal_mode', false) %}
-- Hash the unchanged raw JSON before joining it to canonical values. Only the
-- compact key/hash/contract result crosses the join; no persistent table added.
with raw_rows as (
    select r.source_record_key,r.schema_fingerprint,r.schema_version,
        {{ minimal_raw_values(source('stage','yellow_taxi_raw'),'r') }} as raw_values
    from {{ source('stage','yellow_taxi_raw') }} r
    where r.batch_id={{ minimal_batch_id() }} and r.file_version_id={{ minimal_version_id() }}
    -- Keep one JSON projection per raw row before the serializer references it.
    offset 0
), raw_fields as (
    select source_record_key,schema_fingerprint,schema_version,
        raw_values is not null as raw_values_present,
        {{ minimal_raw_projection_fields() }},
        {{ minimal_raw_contract_valid() }} as raw_contract_valid
    from raw_rows
    offset 0
), raw_hashes as materialized (
    select source_record_key,
        {{ minimal_raw_fingerprint(true) }} as raw_exact_fingerprint,
        raw_contract_valid
    from raw_fields
), joined as (
    select e.*,r.raw_exact_fingerprint,
        coalesce(r.raw_contract_valid,false) as raw_contract_valid
    from {{ ref('int_taxi_trip_enriched') }} e
    left join raw_hashes r on r.source_record_key=e.source_record_key
)
select j.*,
encode(digest(
            {{ fingerprint_field('vendor_id', 1, 'smallint') }} ||
            {{ fingerprint_field('pickup_datetime', 2, 'timestamp') }} ||
            {{ fingerprint_field('dropoff_datetime', 3, 'timestamp') }} ||
            {{ fingerprint_field('pu_location_id', 4, 'smallint') }} ||
            {{ fingerprint_field('do_location_id', 5, 'smallint') }} ||
            {{ fingerprint_field('passenger_count', 6, 'smallint') }} ||
            {{ fingerprint_field('trip_distance', 7, 'numeric(10,3)') }} ||
            {{ fingerprint_field('reported_total_amount', 8, 'numeric(12,2)') }}
            , 'sha256'), 'hex')::char(64) as candidate_trip_fingerprint
from joined j
{% else %}
-- Source field order and logical types come from the approved 2024/2025 survey.
-- Mage must retain the original typed values or equivalent lossless raw_payload.
with raw_rows as (
    select
        r.source_record_key,
        {% if var('dbt_minimal_mode', false) %}
        {{ minimal_raw_values(source('stage', 'yellow_taxi_raw'), 'r') }} as raw_values
        {% else %}
        case when jsonb_typeof(to_jsonb(r) -> 'raw_payload') = 'object'
            then to_jsonb(r) -> 'raw_payload'
            when r.schema_version = 'yt_schema_v001'
                and to_jsonb(r) -> 'cbd_congestion_fee' = 'null'::jsonb
            then to_jsonb(r) - 'cbd_congestion_fee'
            else to_jsonb(r)
        end as raw_values
        {% endif %}
    from {{ source('stage', 'yellow_taxi_raw') }} r
    {% if var('dbt_minimal_mode', false) %}
    where r.batch_id={{ minimal_batch_id() }} and r.file_version_id={{ minimal_version_id() }}
    {% endif %}
), joined as (
    select e.*, r.raw_values
    from {{ ref('int_taxi_trip_enriched') }} e
    left join raw_rows r on r.source_record_key = e.source_record_key
), hashes as (
    select
        j.*,
        case when raw_values is not null then encode(digest(
            trim(schema_fingerprint) ||
            {{ fingerprint_field(raw_source_value('raw_values', 'VendorID'), 1, 'int32') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'tpep_pickup_datetime'), 2, 'timestamp[us]') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'tpep_dropoff_datetime'), 3, 'timestamp[us]') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'passenger_count'), 4, 'int64') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'trip_distance'), 5, 'double') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'RatecodeID'), 6, 'int64') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'store_and_fwd_flag'), 7, 'large_string') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'PULocationID'), 8, 'int32') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'DOLocationID'), 9, 'int32') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'payment_type'), 10, 'int64') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'fare_amount'), 11, 'double') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'extra'), 12, 'double') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'mta_tax'), 13, 'double') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'tip_amount'), 14, 'double') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'tolls_amount'), 15, 'double') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'improvement_surcharge'), 16, 'double') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'total_amount'), 17, 'double') }} ||
            {{ fingerprint_field(raw_source_value('raw_values', 'congestion_surcharge'), 18, 'double') }} ||
            {{ fingerprint_field("coalesce(raw_values ->> 'Airport_fee', raw_values ->> 'airport_fee')", 19, 'double') }}
            || case when {% if var('dbt_minimal_mode', false) %}schema_version = 'yt_schema_v002'{% else %}source_year_month >= '{{ var("cbd_schema_start_month") }}'{% endif %}
                then {{ fingerprint_field(raw_source_value('raw_values', 'cbd_congestion_fee'), 20, 'double') }}
                else '' end
            , 'sha256'), 'hex') end::char(64) as raw_exact_fingerprint,
        encode(digest(
            {{ fingerprint_field('vendor_id', 1, 'smallint') }} ||
            {{ fingerprint_field('pickup_datetime', 2, 'timestamp') }} ||
            {{ fingerprint_field('dropoff_datetime', 3, 'timestamp') }} ||
            {{ fingerprint_field('pu_location_id', 4, 'smallint') }} ||
            {{ fingerprint_field('do_location_id', 5, 'smallint') }} ||
            {{ fingerprint_field('passenger_count', 6, 'smallint') }} ||
            {{ fingerprint_field('trip_distance', 7, 'numeric(10,3)') }} ||
            {{ fingerprint_field('reported_total_amount', 8, 'numeric(12,2)') }}
            , 'sha256'), 'hex')::char(64) as candidate_trip_fingerprint
    from joined j
)
select hashes.*
{% if var('dbt_minimal_mode', false) %}
, coalesce(raw_values is not null and raw_values ?& array[
'VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count',
'trip_distance','RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID',
'payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount',
'improvement_surcharge','total_amount','congestion_surcharge']
and (raw_values ? 'Airport_fee' or raw_values ? 'airport_fee')
and ((schema_version='yt_schema_v001' and not raw_values ? 'cbd_congestion_fee')
  or (schema_version='yt_schema_v002' and raw_values ? 'cbd_congestion_fee')),false) as raw_contract_valid
{% endif %}
from hashes

{% endif %}
