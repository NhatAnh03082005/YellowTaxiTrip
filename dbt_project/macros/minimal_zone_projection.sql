{% macro minimal_zone_projection(relation, alias) -%}
  {# Preserve JSON scalar text/null precedence, without serializing the whole
     lookup row for every join comparison. Column names are loader contracts. #}
  {% set columns=[] %}
  {% if execute %}
    {% for column in adapter.get_columns_in_relation(relation) %}
      {% do columns.append(column.name) %}
    {% endfor %}
  {% endif %}
  {% for name, candidates in [
      ('location_id',['LocationID','location_id']),
      ('borough',['Borough','borough']),
      ('zone_name',['Zone','zone','zone_name']),
      ('service_zone',['service_zone','ServiceZone'])] %}
    coalesce(
    {% for column in candidates if column in columns %}
      to_jsonb({{ alias }}.{{ adapter.quote(column) }}) #>> '{}',
    {% endfor %}
      null::text){% if name=='location_id' %}::smallint{% endif %} as {{ name }}
    {% if not loop.last %},{% endif %}
  {% endfor %}
{%- endmacro %}
