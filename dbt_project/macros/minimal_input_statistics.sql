{% macro minimal_input_statistics() -%}
  {# The staging tables are replaced each file. A stale previous-batch MCV can
     estimate one row for a new batch and choose a quadratic nested loop.
     Refresh inputs under the same writer lock before validation/planning. #}
  select pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'));
  {% set raw = source('stage','yellow_taxi_raw') %}
  {% set relations = [raw, source('stage','yellow_taxi_canonical')] %}
  {% if execute %}
    {% set contract = adapter.get_relation(database=raw.database, schema=raw.schema, identifier='stage_storage_contract') %}
    {% if contract is not none %}
      {% set modes = run_query('select storage_mode from ' ~ contract ~ ' where singleton') %}
      {% if modes.rows | length != 1 or modes.rows[0][0] not in ['slim-v1','slim-v2'] %}
        {{ exceptions.raise_compiler_error('Missing or unsupported Mage staging contract for statistics') }}
      {% endif %}
      {% set names = ['yellow_taxi_trip_data'] if modes.rows[0][0]=='slim-v2' else ['yellow_taxi_raw_data','yellow_taxi_canonical_data'] %}
      {% set relations = [] %}
      {% for name in names + ['stage_file_batch'] %}
        {% do relations.append(api.Relation.create(database=raw.database, schema=raw.schema, identifier=name)) %}
      {% endfor %}
      {% do relations.append(source('control','source_file_version')) %}
    {% endif %}
  {% endif %}
  {% for relation in relations %}
    analyze {{ relation }};
  {% endfor %}
  {% for name in ['vendor_codes','payment_type_codes','rate_codes','dq_rule_policy','zone_key_map'] %}
    analyze {{ ref(name) }};
  {% endfor %}
{%- endmacro %}
