{% macro minimal_batch_id() %}
  {% set value = var('load_batch_id', none) %}
  {% if value is none or value | string != (value | int) | string or value | int <= 0 %}
    {{ exceptions.raise_compiler_error('Minimal mode requires positive integer load_batch_id') }}
  {% endif %}
  {{ return(value | int) }}
{% endmacro %}

{% macro minimal_version_id() %}
  {% set value = var('load_file_version_id', none) %}
  {% if value is none or value | string != (value | int) | string or value | int <= 0 %}
    {{ exceptions.raise_compiler_error('Minimal mode requires positive integer load_file_version_id') }}
  {% endif %}
  {{ return(value | int) }}
{% endmacro %}

{% macro minimal_validate_scope() %}
  {% set batch = minimal_batch_id() %}
  {% set version = minimal_version_id() %}
  {% if var('fact_lifecycle_mode', 'guard') != 'guard'
      or var('enable_fingerprint_history_candidate', false)
      or var('use_fingerprint_history_candidate', false) %}
    {{ exceptions.raise_compiler_error('Minimal mode does not support archive or historical fingerprint flags; use the minimal runner/profile') }}
  {% endif %}
  {% set sql %}
    do $minimal_scope$
    begin
      perform pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'));
      if not exists (select 1 from {{ source('control', 'etl_batch') }}
                     where batch_id = {{ batch }} and status in ('STAGED','DBT_RUNNING','RECONCILING'))
         or not exists (select 1 from {{ source('control', 'source_file_version') }}
                        where file_version_id = {{ version }}) then
        raise exception 'Minimal processing batch/version is missing or not writable';
      end if;
      if (select count(*) from {{ source('control', 'etl_file') }} f
          join {{ source('control','source_file_version') }} v using(file_version_id)
          where f.batch_id={{ batch }} and f.file_version_id={{ version }}
            and f.file_status='STAGED' and f.source_file=v.source_file
            and f.source_file_sha256=v.source_file_sha256) <> 1 then
        raise exception 'Minimal batch requires exactly one matching STAGED file attempt';
      end if;
      {% for name in ['yellow_taxi_raw', 'yellow_taxi_canonical'] %}
      if exists (select 1 from {{ source('stage', name) }} s
                 left join {{ source('control', 'source_file_version') }} v
                   on v.file_version_id = s.file_version_id
                 where s.batch_id is distinct from {{ batch }}
                    or s.file_version_id is distinct from {{ version }}
                    or s.source_file is distinct from v.source_file
                    or s.source_file_sha256 is distinct from v.source_file_sha256
                    or s.source_year_month is distinct from v.source_year_month
                    or s.schema_version is distinct from v.schema_version
                    or s.schema_fingerprint is distinct from v.schema_fingerprint
                    or s.source_row_number is null or s.source_row_number < 1 or s.source_record_key is null
                    or s.source_record_key is distinct from
                       encode(digest(lower(s.source_file_sha256)::text || ':' || s.source_row_number::text,'sha256'),'hex')) then
        raise exception 'Mixed or inconsistent minimal staging scope: {{ name }}';
      end if;
      {% endfor %}
      if exists (select 1 from {{ source('mage_dq', 'technical_quarantine') }}
                 where batch_id = {{ batch }} and file_version_id is distinct from {{ version }}) then
        raise exception 'Mixed minimal technical-reject scope';
      end if;
      {% if var('apply_approved_fact_replacement', false) %}
      if (select count(*) from ({{ fact_replacement_scope(batch) }}) s
          where new_version_id = {{ version }} and old_version_id <> new_version_id) <> 1 then
        raise exception 'Minimal replacement requires exactly one valid old/new binding';
      end if;
      {% else %}
      if exists (
        select 1 from {{ source('control', 'etl_file') }} f
        join {{ source('control', 'etl_batch') }} b using (batch_id)
        join {{ source('control', 'etl_batch') }} current_batch on current_batch.batch_id = {{ batch }}
        join {{ source('control', 'source_file_version') }} v on v.file_version_id = {{ version }}
        where b.status = 'SUCCESS' and b.pipeline_name = current_batch.pipeline_name
          and b.source_name = current_batch.source_name and f.source_file = v.source_file
          and f.file_version_id <> {{ version }}
      ) then
        raise exception 'Changed source version requires explicit approved replacement';
      end if;
      {% endif %}
    end $minimal_scope$;
  {% endset %}
  {{ return(sql) }}
{% endmacro %}

{% macro minimal_invalidate_workspace() %}
  {# Intentionally commit ONLY invalidation before the table-build transaction.
     A failed rebuild must never leave a previously valid same-batch workspace usable. #}
  {% set sql %}
    do $invalidate_workspace$
    begin
      perform pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'));
      if to_regclass('{{ this.schema }}.dbt_minimal_workspace_state') is not null then
        alter table {{ this.schema }}.dbt_minimal_workspace_state
          add column if not exists valid boolean not null default false;
        update {{ this.schema }}.dbt_minimal_workspace_state set valid=false
          where model_name='int_taxi_trip_classified';
      end if;
    end $invalidate_workspace$;
    commit;
  {% endset %}
  {{ return(sql) }}
{% endmacro %}

{% macro minimal_record_workspace() %}
  {% set sql %}
    alter table {{ this }} add constraint minimal_raw_input_valid
      check (raw_contract_valid is true and raw_exact_fingerprint is not null and not has_unmapped_rule);
    create table if not exists {{ this.schema }}.dbt_minimal_workspace_state (
      model_name text primary key, batch_id bigint not null, file_version_id bigint not null,
      built_at timestamptz not null default clock_timestamp(),valid boolean not null default true
    );
    alter table {{ this.schema }}.dbt_minimal_workspace_state add column if not exists valid boolean not null default false;
    insert into {{ this.schema }}.dbt_minimal_workspace_state(model_name,batch_id,file_version_id,valid)
      values ('int_taxi_trip_classified',{{ minimal_batch_id() }},{{ minimal_version_id() }},true)
      on conflict (model_name) do update set batch_id=excluded.batch_id,
        file_version_id=excluded.file_version_id,built_at=clock_timestamp(),valid=true;
    create table if not exists {{ this.schema }}.dbt_minimal_candidate_summary (
      batch_id bigint not null, file_version_id bigint not null,
      candidate_rows bigint not null, candidate_groups bigint not null,
      primary key(batch_id,file_version_id)
    );
    insert into {{ this.schema }}.dbt_minimal_candidate_summary
    select {{ minimal_batch_id() }}, {{ minimal_version_id() }},
           count(*) filter(where has_in_file_candidate_match),
           count(distinct candidate_trip_fingerprint) filter(where has_in_file_candidate_match)
    from {{ this }}
    on conflict(batch_id,file_version_id) do update set
      candidate_rows=excluded.candidate_rows,candidate_groups=excluded.candidate_groups;
    analyze {{ this }};
  {% endset %}
  {{ return(sql) }}
{% endmacro %}

{% macro minimal_assert_workspace() %}
  {% set relation = ref('int_taxi_trip_classified') %}
  {% set sql %}
    do $minimal_workspace$
    begin
      if not exists(select 1 from {{ relation.schema }}.dbt_minimal_workspace_state
        where model_name='int_taxi_trip_classified'
          and batch_id={{ minimal_batch_id() }} and file_version_id={{ minimal_version_id() }} and valid) then
        raise exception 'Classification workspace belongs to another batch/version';
      end if;
    end $minimal_workspace$;
  {% endset %}
  {{ return(sql) }}
{% endmacro %}
