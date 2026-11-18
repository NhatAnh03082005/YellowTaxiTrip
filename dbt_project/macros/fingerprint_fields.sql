{% macro fingerprint_field(value_sql, ordinal, type_tag) -%}
    ('{{ ordinal }}:{{ type_tag }}:' || case
        when {{ value_sql }} is null then 'N;'
        else 'V' || octet_length(convert_to(({{ value_sql }})::text, 'UTF8'))::text
            || ':' || ({{ value_sql }})::text || ';'
    end)
{%- endmacro %}

{% macro raw_source_value(payload_sql, column_name) -%}
    ({{ payload_sql }} ->> '{{ column_name }}')
{%- endmacro %}
