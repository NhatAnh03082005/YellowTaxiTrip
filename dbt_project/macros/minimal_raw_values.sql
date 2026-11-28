{% macro minimal_raw_values(relation, alias) -%}
  {# Explicit raw_payload objects retain priority and remain unchanged. For
     native columns, serialize only the exact keys consumed by the existing
     fingerprint/contract macros; metadata is not part of either contract. #}
  {% set columns=[] %}
  {% if execute %}
    {% for column in adapter.get_columns_in_relation(relation) %}
      {% do columns.append(column.name) %}
    {% endfor %}
  {% endif %}
  {% set native_fields=[] %}
  {% for name in ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime',
      'passenger_count','trip_distance','RatecodeID','store_and_fwd_flag',
      'PULocationID','DOLocationID','payment_type','fare_amount','extra',
      'mta_tax','tip_amount','tolls_amount','improvement_surcharge',
      'total_amount','congestion_surcharge','Airport_fee','airport_fee',
      'cbd_congestion_fee'] if name in columns %}
    {% do native_fields.append("'" ~ name ~ "'," ~ alias ~ '.' ~ adapter.quote(name)) %}
  {% endfor %}
  {% set native_values = 'jsonb_build_object(' ~ native_fields | join(',') ~ ')'
      if execute else 'to_jsonb(' ~ alias ~ ')' %}
  case
    {% if 'raw_payload' in columns %}
    when jsonb_typeof(to_jsonb({{ alias }}.raw_payload))='object'
      then to_jsonb({{ alias }}.raw_payload)
    {% endif %}
    {% if 'cbd_congestion_fee' in columns %}
    when {{ alias }}.schema_version='yt_schema_v001'
      and coalesce(to_jsonb({{ alias }}.cbd_congestion_fee),'null'::jsonb)='null'::jsonb
      then {{ native_values }}-'cbd_congestion_fee'
    {% endif %}
    {% if 'raw_payload' not in columns and 'cbd_congestion_fee' not in columns %}
    when false then null::jsonb
    {% endif %}
    else {{ native_values }}
  end
{%- endmacro %}
