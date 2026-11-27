{% macro minimal_raw_values(relation, alias) -%}
  {# Keep the exact original JSON object and hash input. Inspect column presence
     at compile time instead of serializing the whole row to inspect two keys. #}
  {% set columns=[] %}
  {% if execute %}
    {% for column in adapter.get_columns_in_relation(relation) %}
      {% do columns.append(column.name) %}
    {% endfor %}
  {% endif %}
  case
    {% if 'raw_payload' in columns %}
    when jsonb_typeof(to_jsonb({{ alias }}.raw_payload))='object'
      then to_jsonb({{ alias }}.raw_payload)
    {% endif %}
    {% if 'cbd_congestion_fee' in columns %}
    when {{ alias }}.schema_version='yt_schema_v001'
      and coalesce(to_jsonb({{ alias }}.cbd_congestion_fee),'null'::jsonb)='null'::jsonb
      then to_jsonb({{ alias }})-'cbd_congestion_fee'
    {% endif %}
    {% if 'raw_payload' not in columns and 'cbd_congestion_fee' not in columns %}
    when false then null::jsonb
    {% endif %}
    else to_jsonb({{ alias }})
  end
{%- endmacro %}
