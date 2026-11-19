{% materialization minimal_reference, adapter='postgres' %}
  {% set relation=this.incorporate(type='table') %}
  {% set key=config.require('reference_key') %}
  {% set natural=config.get('reference_natural_key', none) %}
  {% set signature_parts=[local_md5(sql)] %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {% call statement('reference_begin', auto_begin=True) %}
    select pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'))
  {% endcall %}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% for dependency in config.get('reference_dependencies',[]) %}
    {% set checksum=run_query("select md5(coalesce(string_agg(to_jsonb(t)::text,'|' order by to_jsonb(t)::text),'')) from " ~ ref(dependency) ~ ' t') %}
    {% do signature_parts.append(checksum.rows[0][0]) %}
  {% endfor %}
  {% set signature=local_md5(signature_parts | join(':')) %}
  {% set state=adapter.quote(this.schema) ~ '.dbt_minimal_reference_state' %}
  {% do run_query('create table if not exists ' ~ state ~ '(model_name text primary key,signature char(32) not null,refreshed_at timestamptz not null default clock_timestamp())') %}
  {% set existing=adapter.get_relation(database=this.database,schema=this.schema,identifier=this.identifier) %}
  {% set previous=run_query('select signature from ' ~ state ~ " where model_name='" ~ this.identifier ~ "'") %}
  {% set unchanged=existing is not none and previous.rows | length == 1 and previous.rows[0][0] == signature %}
  {% if not unchanged %}
    {% if existing is not none %}
      {% if existing.type != 'table' %}{{ exceptions.raise_compiler_error('Minimal reference target must be table') }}{% endif %}
      {% do minimal_require_model_columns(relation,sql) %}
    {% endif %}
    {% do run_query('create temporary table minimal_reference_source on commit drop as ' ~ sql) %}
    {% do run_query('create unique index on minimal_reference_source(' ~ adapter.quote(key) ~ ')') %}
    {% if existing is none %}
      {% do run_query('create table ' ~ relation ~ ' as select * from minimal_reference_source with no data') %}
      {% do run_query('alter table ' ~ relation ~ ' add primary key(' ~ adapter.quote(key) ~ ')') %}
      {% if natural %}{% do run_query('create unique index on ' ~ relation ~ '(' ~ adapter.quote(natural) ~ ')') %}{% endif %}
    {% elif natural %}
      {% set drift=run_query('select 1 from ' ~ relation ~ ' r join minimal_reference_source s on r.' ~ adapter.quote(natural) ~ '=s.' ~ adapter.quote(natural) ~ ' where r.' ~ adapter.quote(key) ~ ' <> s.' ~ adapter.quote(key) ~ ' limit 1') %}
      {% if drift.rows | length %}{{ exceptions.raise_compiler_error('Reference surrogate mapping drift; explicit migration required') }}{% endif %}
    {% endif %}
    {% if existing is not none and key == 'date_key' %}
      {% set shrinking=run_query('select 1 from ' ~ relation ~ ' r where r.date_key>0 and not exists(select 1 from minimal_reference_source s where s.date_key=r.date_key) limit 1') %}
      {% if shrinking.rows | length %}{{ exceptions.raise_compiler_error('Refusing to shrink existing calendar; expand configured minimal_calendar range') }}{% endif %}
    {% endif %}
    {% set names=[] %}{% set assignments=[] %}{% set old=[] %}{% set new=[] %}
    {% for column in adapter.get_columns_in_relation(relation) %}
      {% do names.append(adapter.quote(column.name)) %}
      {% if column.name != key %}
        {% do assignments.append(adapter.quote(column.name) ~ '=excluded.' ~ adapter.quote(column.name)) %}
        {% do old.append('r.' ~ adapter.quote(column.name)) %}
        {% do new.append('excluded.' ~ adapter.quote(column.name)) %}
      {% endif %}
    {% endfor %}
    {% do run_query('delete from ' ~ relation ~ ' r where not exists(select 1 from minimal_reference_source s where s.' ~ adapter.quote(key) ~ '=r.' ~ adapter.quote(key) ~ ')') %}
    {% call statement('main') %}
      insert into {{ relation }} as r ({{ names | join(',') }})
      select {{ names | join(',') }} from minimal_reference_source where true
      on conflict({{ adapter.quote(key) }}) do update set {{ assignments | join(',') }}
      where row({{ old | join(',') }}) is distinct from row({{ new | join(',') }})
    {% endcall %}
    {% do run_query('analyze ' ~ relation) %}
    {% do run_query('insert into ' ~ state ~ "(model_name,signature) values ('" ~ this.identifier ~ "','" ~ signature ~ "') on conflict(model_name) do update set signature=excluded.signature,refreshed_at=clock_timestamp()") %}
  {% else %}
    {% call statement('main') %}select 1 as reference_unchanged{% endcall %}
  {% endif %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations':[relation]}) }}
{% endmaterialization %}
