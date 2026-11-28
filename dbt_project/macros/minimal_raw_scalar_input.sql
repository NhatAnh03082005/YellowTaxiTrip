{% macro minimal_raw_scalar_source(relation, alias) -%}
  {# Preserve JSON's scalar text representation without constructing a native
     whole-row object. Explicit object payloads still override every native value. #}
  {% set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list %}
  {% set fields = ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime',
      'passenger_count','trip_distance','RatecodeID','store_and_fwd_flag',
      'PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax',
      'tip_amount','tolls_amount','improvement_surcharge','total_amount',
      'congestion_surcharge','Airport_fee','cbd_congestion_fee'] %}
  {% if 'raw_payload' in columns %}
  case when jsonb_typeof(to_jsonb({{ alias }}.raw_payload))='object'
       then to_jsonb({{ alias }}.raw_payload) end as raw_values,
  {% else %}null::jsonb as raw_values,{% endif %}
  {% for name in fields %}
    {% if name == 'Airport_fee' %}
      coalesce(
      {% if 'Airport_fee' in columns %}to_jsonb({{ alias }}.{{ adapter.quote('Airport_fee') }}) #>> '{}'{% else %}null::text{% endif %},
      {% if 'airport_fee' in columns %}to_jsonb({{ alias }}.{{ adapter.quote('airport_fee') }}) #>> '{}'{% else %}null::text{% endif %})
    {% elif name in columns %}
      to_jsonb({{ alias }}.{{ adapter.quote(name) }}) #>> '{}'
    {% else %}null::text{% endif %} as native_value_{{ loop.index }},
  {% endfor %}
  {% set ns = namespace(complete=true) %}
  {% for name in fields[:18] if name not in columns %}{% set ns.complete=false %}{% endfor %}
  coalesce({{ 'true' if ns.complete and ('Airport_fee' in columns or 'airport_fee' in columns) else 'false' }}
    and (({{ alias }}.schema_version='yt_schema_v001' and
      {% if 'cbd_congestion_fee' in columns %}{{ alias }}.cbd_congestion_fee is null{% else %}true{% endif %})
      or ({{ alias }}.schema_version='yt_schema_v002' and
      {{ 'true' if 'cbd_congestion_fee' in columns else 'false' }})),false) as native_contract_valid
{%- endmacro %}

{% macro minimal_raw_scalar_fields() -%}
  {% for name in ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime',
      'passenger_count','trip_distance','RatecodeID','store_and_fwd_flag',
      'PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax',
      'tip_amount','tolls_amount','improvement_surcharge','total_amount',
      'congestion_surcharge','Airport_fee','cbd_congestion_fee'] %}
    case when raw_values is not null then
      {% if name == 'Airport_fee' %}coalesce(raw_values ->> 'Airport_fee',raw_values ->> 'airport_fee')
      {% else %}{{ raw_source_value('raw_values',name) }}{% endif %}
      else native_value_{{ loop.index }} end as raw_value_{{ loop.index }},
  {% endfor %}
  case when raw_values is not null then {{ minimal_raw_contract_valid() }}
       else native_contract_valid end as raw_contract_valid
{%- endmacro %}
