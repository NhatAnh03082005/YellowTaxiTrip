{# Exact serializer copied from the existing implementation; keep ordinal/type tags unchanged. #}
{% macro minimal_raw_fingerprint(from_fields=false) -%}
case when {% if from_fields %}raw_values_present{% else %}raw_values is not null{% endif %} then encode(digest(
            trim(schema_fingerprint) ||
            {{ fingerprint_field(('raw_value_1' if from_fields else raw_source_value('raw_values', 'VendorID')), 1, 'int32') }} ||
            {{ fingerprint_field(('raw_value_2' if from_fields else raw_source_value('raw_values', 'tpep_pickup_datetime')), 2, 'timestamp[us]') }} ||
            {{ fingerprint_field(('raw_value_3' if from_fields else raw_source_value('raw_values', 'tpep_dropoff_datetime')), 3, 'timestamp[us]') }} ||
            {{ fingerprint_field(('raw_value_4' if from_fields else raw_source_value('raw_values', 'passenger_count')), 4, 'int64') }} ||
            {{ fingerprint_field(('raw_value_5' if from_fields else raw_source_value('raw_values', 'trip_distance')), 5, 'double') }} ||
            {{ fingerprint_field(('raw_value_6' if from_fields else raw_source_value('raw_values', 'RatecodeID')), 6, 'int64') }} ||
            {{ fingerprint_field(('raw_value_7' if from_fields else raw_source_value('raw_values', 'store_and_fwd_flag')), 7, 'large_string') }} ||
            {{ fingerprint_field(('raw_value_8' if from_fields else raw_source_value('raw_values', 'PULocationID')), 8, 'int32') }} ||
            {{ fingerprint_field(('raw_value_9' if from_fields else raw_source_value('raw_values', 'DOLocationID')), 9, 'int32') }} ||
            {{ fingerprint_field(('raw_value_10' if from_fields else raw_source_value('raw_values', 'payment_type')), 10, 'int64') }} ||
            {{ fingerprint_field(('raw_value_11' if from_fields else raw_source_value('raw_values', 'fare_amount')), 11, 'double') }} ||
            {{ fingerprint_field(('raw_value_12' if from_fields else raw_source_value('raw_values', 'extra')), 12, 'double') }} ||
            {{ fingerprint_field(('raw_value_13' if from_fields else raw_source_value('raw_values', 'mta_tax')), 13, 'double') }} ||
            {{ fingerprint_field(('raw_value_14' if from_fields else raw_source_value('raw_values', 'tip_amount')), 14, 'double') }} ||
            {{ fingerprint_field(('raw_value_15' if from_fields else raw_source_value('raw_values', 'tolls_amount')), 15, 'double') }} ||
            {{ fingerprint_field(('raw_value_16' if from_fields else raw_source_value('raw_values', 'improvement_surcharge')), 16, 'double') }} ||
            {{ fingerprint_field(('raw_value_17' if from_fields else raw_source_value('raw_values', 'total_amount')), 17, 'double') }} ||
            {{ fingerprint_field(('raw_value_18' if from_fields else raw_source_value('raw_values', 'congestion_surcharge')), 18, 'double') }} ||
            {{ fingerprint_field(('raw_value_19' if from_fields else "coalesce(raw_values ->> 'Airport_fee', raw_values ->> 'airport_fee')"), 19, 'double') }}
            || case when {% if var('dbt_minimal_mode', false) %}schema_version = 'yt_schema_v002'{% else %}source_year_month >= '{{ var("cbd_schema_start_month") }}'{% endif %}
                then {{ fingerprint_field(('raw_value_20' if from_fields else raw_source_value('raw_values', 'cbd_congestion_fee')), 20, 'double') }}
                else '' end
            , 'sha256'), 'hex') end::char(64)
{%- endmacro %}

{% macro minimal_raw_contract_valid() -%}
coalesce(raw_values is not null and raw_values ?& array[
'VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count',
'trip_distance','RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID',
'payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount',
'improvement_surcharge','total_amount','congestion_surcharge']
and (raw_values ? 'Airport_fee' or raw_values ? 'airport_fee')
and ((schema_version='yt_schema_v001' and not raw_values ? 'cbd_congestion_fee')
  or (schema_version='yt_schema_v002' and raw_values ? 'cbd_congestion_fee')),false)
{%- endmacro %}

{% macro minimal_raw_projection_fields() -%}
  {% for name in ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance','RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge','Airport_fee','cbd_congestion_fee'] %}
    {% if name=='Airport_fee' %}coalesce(raw_values ->> 'Airport_fee',raw_values ->> 'airport_fee')
    {% else %}{{ raw_source_value('raw_values',name) }}{% endif %} as raw_value_{{ loop.index }}
    {% if not loop.last %},{% endif %}
  {% endfor %}
{%- endmacro %}
