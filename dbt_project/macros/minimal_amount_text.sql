{% macro minimal_amount_text(relation, alias, field, technical=false) -%}
  {# Read only the requested scalar/payload key. Whole-row to_jsonb serializes
     every source column repeatedly; preserve its text/null and fallback order. #}
  {% set columns=[] %}
  {% if execute %}
    {% for column in adapter.get_columns_in_relation(relation) %}
      {% do columns.append(column.name) %}
    {% endfor %}
  {% endif %}
  {% set values=[] %}
  {% if technical and 'raw_payload' in columns %}
    {% do values.append(alias ~ ".raw_payload->>'" ~ field ~ "_amount'") %}
  {% endif %}
  {% if field ~ '_amount' in columns %}
    {% do values.append("(to_jsonb(" ~ alias ~ '.' ~ adapter.quote(field ~ '_amount') ~ ")#>>'{}')") %}
  {% endif %}
  {% if not technical and 'raw_payload' in columns %}
    {% do values.append(alias ~ ".raw_payload->>'" ~ field ~ "_amount'") %}
  {% endif %}
  {% if technical and field == 'total' and 'total_amount_raw' in columns %}
    {% do values.append("(to_jsonb(" ~ alias ~ ".total_amount_raw)#>>'{}')") %}
  {% endif %}
  {% if values | length > 1 %}coalesce({{ values | join(', ') }})
  {% elif values | length == 1 %}{{ values[0] }}
  {% else %}null::text{% endif %}
{%- endmacro %}
