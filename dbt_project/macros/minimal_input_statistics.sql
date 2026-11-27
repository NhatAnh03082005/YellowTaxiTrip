{% macro minimal_input_statistics() -%}
  {# The staging tables are replaced each file. A stale previous-batch MCV can
     estimate one row for a new batch and choose a quadratic nested loop.
     Refresh inputs under the same writer lock before validation/planning. #}
  select pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'));
  analyze {{ source('stage','yellow_taxi_raw') }};
  analyze {{ source('stage','yellow_taxi_canonical') }};
  {% for name in ['vendor_codes','payment_type_codes','rate_codes','dq_rule_policy','zone_key_map'] %}
    analyze {{ ref(name) }};
  {% endfor %}
{%- endmacro %}
