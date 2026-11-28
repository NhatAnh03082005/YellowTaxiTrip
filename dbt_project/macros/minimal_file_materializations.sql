{% macro minimal_require_model_columns(relation, sql) %}
  {% set probe = run_query('select * from (' ~ sql ~ ') as model_columns limit 0') %}
  {% set actual = adapter.get_columns_in_relation(relation) | map(attribute='name') | list %}
  {% set wanted = probe.column_names | list %}
  {% if relation.identifier == 'fact_taxi_trip' %}
    {% if 'trip_key' not in actual %}{{ exceptions.raise_compiler_error('Minimal Fact requires identity trip_key; explicit migration required') }}{% endif %}
    {% do actual.remove('trip_key') %}
  {% endif %}
  {% do run_query('create temporary table minimal_shape_probe on commit drop as select * from (' ~ sql ~ ') q with no data') %}
  {% set type_check %}
    select a.attname from pg_attribute a
    join pg_attribute p on p.attrelid='pg_temp.minimal_shape_probe'::regclass and p.attname=a.attname
    where a.attrelid='{{ adapter.quote(relation.schema) }}.{{ adapter.quote(relation.identifier) }}'::regclass
      and a.attnum>0 and not a.attisdropped and p.attnum>0 and not p.attisdropped
      and format_type(a.atttypid,a.atttypmod) <> format_type(p.atttypid,p.atttypmod)
  {% endset %}
  {% if run_query(type_check).rows | length %}
    {{ exceptions.raise_compiler_error('Minimal column types differ for ' ~ relation ~ '; explicit migration required') }}
  {% endif %}
  {% if actual | sort != wanted | sort %}
    {{ exceptions.raise_compiler_error('Minimal schema differs for ' ~ relation ~
       '; use the reviewed T09 migration on a non-live target first. No automatic column drop/full-refresh.') }}
  {% endif %}
{% endmacro %}

{% macro minimal_delete_replaced_version(relation) %}
  {% if var('apply_approved_fact_replacement', false) %}
    delete from {{ relation }}
    where file_version_id in (select old_version_id from ({{ fact_replacement_scope(minimal_batch_id()) }}) s
                             where new_version_id={{ minimal_version_id() }})
  {% else %}
    select 1
  {% endif %}
{% endmacro %}

{% materialization minimal_fact, adapter='postgres' %}
  {% set relation = this.incorporate(type='table') %}
  {% if should_full_refresh() %}
    {{ exceptions.raise_compiler_error('Minimal Fact forbids --full-refresh; use explicit migration') }}
  {% endif %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {% call statement('minimal_begin', auto_begin=True) %}
    select pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'))
  {% endcall %}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% do run_query(minimal_validate_scope()) %}
  {% do run_query(minimal_assert_workspace()) %}
  {% set existing = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
  {% if existing is not none %}
    {% if existing.type != 'table' %}{{ exceptions.raise_compiler_error('Minimal Fact target must be a table') }}{% endif %}
    {% do minimal_require_model_columns(relation, sql) %}
  {% endif %}
  {% if existing is none %}
    {# Initial target: stream the model into Fact once. Target UNIQUE/PK are
       still built before post-hooks/commit, so duplicate input rolls back. #}
    {% do run_query('create temporary view minimal_fact_source as ' ~ sql) %}
  {% else %}
    {# Keep source heap/index locality for subsequent files and retries. This
       changes insertion order only; every row and conflict check is retained. #}
    {% do run_query('create temporary table minimal_fact_source on commit drop as select * from (' ~ sql ~ ') ordered_source order by source_record_key') %}
    {% do run_query('create unique index on minimal_fact_source(source_record_key)') %}
    {% do run_query('analyze minimal_fact_source') %}
  {% endif %}
  {% do run_query("do $$ begin if exists(select 1 from minimal_fact_source where pickup_date_key=0 or dropoff_date_key=0) then raise exception 'Valid source date outside calendar; extend minimal_calendar range before Fact load'; end if; end $$") %}
  {% if existing is none %}
    {% do run_query('create table ' ~ relation ~ ' as select * from minimal_fact_source with no data') %}
    {% do run_query('alter table ' ~ relation ~ ' add column trip_key bigint generated always as identity') %}
    {% do run_query('alter table ' ~ relation ~ ' alter column source_record_key set not null') %}
    {% for name in ['file_version_id','source_row_number','batch_id','trip_count',
       'pickup_date_key','dropoff_date_key','pickup_time_key','dropoff_time_key',
       'pickup_zone_key','dropoff_zone_key','vendor_key','payment_type_key','rate_code_key',
       'is_revenue_kpi_eligible','is_tip_kpi_eligible','is_speed_kpi_eligible',
       'is_duration_kpi_eligible','is_passenger_kpi_eligible'] %}
      {% do run_query('alter table ' ~ relation ~ ' alter column ' ~ name ~ ' set not null') %}
    {% endfor %}
  {% endif %}
  {% if existing is not none %}
    {# The unique source-key lookup already performed by ON CONFLICT is the
       collision check for ordinary loads/retries. Raise inside that same
       transaction instead of separately joining all historical Fact keys.
       Approved replacement still checks before deleting its old version. #}
    {% do run_query(minimal_create_fact_version_guard()) %}
  {% endif %}
  {% if existing is not none and var('apply_approved_fact_replacement', false) %}
    {% set collisions=run_query('select 1 from ' ~ relation ~ ' f join minimal_fact_source s using(source_record_key) where f.file_version_id <> s.file_version_id limit 1') %}
    {% if collisions.rows | length %}
      {{ exceptions.raise_compiler_error('Source identity collision across file versions; refusing to overwrite unrelated Fact') }}
    {% endif %}
  {% endif %}
  {# Retain old+new affected grains across a failed downstream retry. No Fact image/archive. #}
  {% do run_query('create table if not exists ' ~ adapter.quote(this.schema) ~
    '.dbt_minimal_affected_grains(batch_id bigint not null,file_version_id bigint not null,' ~
    'pickup_date_key integer not null,source_year_month char(7) not null,' ~
    'primary key(batch_id,file_version_id,pickup_date_key,source_year_month))') %}
  {% set impact_sql %}
    insert into {{ adapter.quote(this.schema) }}.dbt_minimal_affected_grains
    select distinct {{ minimal_batch_id() }}, f.file_version_id, f.pickup_date_key, v.source_year_month
    from {{ relation }} f join {{ source('control','source_file_version') }} v using(file_version_id)
    where f.file_version_id={{ minimal_version_id() }}
    {% if var('apply_approved_fact_replacement', false) %}
      or f.file_version_id in (select old_version_id from ({{ fact_replacement_scope(minimal_batch_id()) }}) s)
    {% endif %}
    union
    select distinct {{ minimal_batch_id() }}, s.file_version_id,s.pickup_date_key,v.source_year_month
    from minimal_fact_source s join {{ source('control','source_file_version') }} v using(file_version_id) where true
    on conflict do nothing
  {% endset %}
  {% do run_query(impact_sql) %}
  {% do run_query(minimal_delete_replaced_version(relation)) %}
  {% do run_query('delete from ' ~ relation ~ ' f where f.file_version_id=' ~ minimal_version_id() ~
    ' and not exists(select 1 from minimal_fact_source s where s.source_record_key=f.source_record_key)') %}
  {% set columns = adapter.get_columns_in_relation(relation) %}
  {% set names=[] %}{% set assignments=[] %}{% set current=[] %}{% set incoming=[] %}
  {% for column in columns if column.name != 'trip_key' %}
    {% do names.append(adapter.quote(column.name)) %}
    {% if column.name != 'source_record_key' %}
      {% if column.name == 'file_version_id' %}
        {% do assignments.append(adapter.quote(column.name) ~ '=pg_temp.minimal_fact_version_guard(f.file_version_id,excluded.file_version_id)') %}
      {% else %}
        {% do assignments.append(adapter.quote(column.name) ~ '=excluded.' ~ adapter.quote(column.name)) %}
      {% endif %}
      {% do current.append('f.' ~ adapter.quote(column.name)) %}
      {% do incoming.append('excluded.' ~ adapter.quote(column.name)) %}
    {% endif %}
  {% endfor %}
  {% call statement('main') %}
    insert into {{ relation }} as f ({{ names | join(',') }})
    select {{ names | join(',') }} from minimal_fact_source where true
    {% if existing is not none %}
    order by source_record_key
    on conflict(source_record_key) do update set {{ assignments | join(',') }}
    where row({{ current | join(',') }}) is distinct from row({{ incoming | join(',') }})
    {% endif %}
  {% endcall %}
  {% if existing is not none %}
    {% do run_query('drop function pg_temp.minimal_fact_version_guard(bigint,bigint)') %}
  {% endif %}
  {% if existing is none %}
    {# New target only: build indexes in bulk, still inside the same writer lock
       and transaction. Existing targets retain their constraints and upsert. #}
    {% do run_query('alter table ' ~ relation ~ ' add primary key(trip_key)') %}
    {% do run_query('alter table ' ~ relation ~ ' add constraint fact_taxi_trip_source_record_key_key unique(source_record_key)') %}
    {% do run_query('create index on ' ~ relation ~ '(file_version_id)') %}
  {% endif %}
  {% do run_query('analyze ' ~ relation) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% if existing is none %}
    {% do run_query('drop view pg_temp.minimal_fact_source') %}
  {% endif %}
  {% do adapter.commit() %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations':[relation]}) }}
{% endmaterialization %}

{% materialization minimal_file_replace, adapter='postgres' %}
  {% set relation=this.incorporate(type='table') %}
  {% if should_full_refresh() %}{{ exceptions.raise_compiler_error('Minimal DQ requires scoped load, not full-refresh') }}{% endif %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {% call statement('minimal_begin', auto_begin=True) %}
    select pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'))
  {% endcall %}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% do run_query(minimal_validate_scope()) %}
  {% do run_query(minimal_assert_workspace()) %}
  {% set existing=adapter.get_relation(database=this.database,schema=this.schema,identifier=this.identifier) %}
  {% if existing is none %}
    {% do run_query('create table ' ~ relation ~ ' as select * from (' ~ sql ~ ') q with no data') %}
    {% do run_query('alter table ' ~ relation ~ ' alter column source_record_key set not null') %}
    {% do run_query('create unique index on ' ~ relation ~ '(source_record_key)') %}
    {% do run_query('create index on ' ~ relation ~ '(file_version_id)') %}
  {% else %}
    {% if existing.type != 'table' %}{{ exceptions.raise_compiler_error('Minimal DQ target must be table') }}{% endif %}
    {% do minimal_require_model_columns(relation,sql) %}
  {% endif %}
  {% do run_query(minimal_delete_replaced_version(relation)) %}
  {% do run_query('delete from ' ~ relation ~ ' where file_version_id=' ~ minimal_version_id()) %}
  {% set model_columns = run_query('select * from (' ~ sql ~ ') q limit 0').column_names %}
  {% set names=[] %}
  {% for name in model_columns %}{% do names.append(adapter.quote(name)) %}{% endfor %}
  {% call statement('main') %}insert into {{ relation }} ({{ names | join(',') }}) {{ sql }}{% endcall %}
  {% do run_query('analyze ' ~ relation) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations':[relation]}) }}
{% endmaterialization %}
