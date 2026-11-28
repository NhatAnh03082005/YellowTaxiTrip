{% macro minimal_assert_runtime_settings() %}
  {% set expected = env_var('DBT_WORK_MEM_MB', '') %}
  {% if execute and var('dbt_minimal_mode', false) and expected %}
    {% if expected | string != (expected | int) | string or expected | int <= 0 %}
      {{ exceptions.raise_compiler_error('DBT_WORK_MEM_MB requires a positive integer') }}
    {% endif %}
    {% set settings = run_query("select current_setting('work_mem'), pg_size_bytes(current_setting('work_mem'))") %}
    {% if settings.rows[0][1] | int != (expected | int) * 1024 * 1024 %}
      {{ exceptions.raise_compiler_error('dbt session work_mem does not match DBT_WORK_MEM_MB; profile connection options may override PGOPTIONS') }}
    {% endif %}
    {% do log('DDS session work_mem=' ~ settings.rows[0][0] ~ ' (connection setting; no server change)', info=true) %}
  {% endif %}
  select 1
{% endmacro %}
