{% materialization fact_identity, adapter='postgres' %}
    {% set target_relation = this.incorporate(type='table') %}
    {% if should_full_refresh() %}
        {{ exceptions.raise_compiler_error('fact_taxi_trip does not support --full-refresh because it would reassign trip_key') }}
    {% endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set lifecycle_mode = var('fact_lifecycle_mode', 'guard') %}
    {% if lifecycle_mode not in ['guard', 'archive'] %}
        {{ exceptions.raise_compiler_error('fact_lifecycle_mode must be guard or archive') }}
    {% endif %}
    {% set history_schema = var('fact_lifecycle_history_schema', 'dq') %}
    {% set history_table = var('fact_lifecycle_history_table', 'fact_taxi_trip_history') %}
    {% set history_relation = adapter.quote(history_schema) ~ '.' ~ adapter.quote(history_table) %}
    {# Serialize Fact writers, including the first table creation. #}
    {% do run_query("select pg_advisory_xact_lock(hashtext('yellow_taxi_fact_identity'))") %}

    {% set existing_relation = adapter.get_relation(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=target_relation.identifier
    ) %}
    {% if existing_relation is not none and existing_relation.type != 'table' %}
        {{ exceptions.raise_compiler_error('fact_taxi_trip already exists but is not a table') }}
    {% endif %}
    {% if lifecycle_mode == 'guard' and adapter.get_relation(
        database=target_relation.database, schema=history_schema,
        identifier=history_table) is not none %}
        {{ exceptions.raise_compiler_error(
            'Fact lifecycle history already exists; run with fact_lifecycle_mode: archive to avoid untracked updates'
        ) }}
    {% endif %}

    {% if existing_relation is not none and lifecycle_mode == 'guard' %}
        {# Guard mode is the safe default for databases not yet migrated. #}
        {% set stale_fact_probe = run_query(
            'select f.source_record_key from ' ~ target_relation ~ ' f ' ~
            'where f.file_version_id in (select distinct file_version_id from ' ~
            ref('int_taxi_trip_classified') ~ ') and not exists (select 1 from ' ~
            ref('int_taxi_trip_accepted') ~
            ' a where a.source_record_key = f.source_record_key) limit 1'
        ) %}
        {% if stale_fact_probe.rows | length > 0 %}
            {{ exceptions.raise_compiler_error(
                'fact_taxi_trip contains a source key no longer accepted: ' ~
                stale_fact_probe.rows[0][0] ~
                '. Use reviewed archive mode after validating the source and migration plan.'
            ) }}
        {% endif %}
    {% endif %}

    {% if existing_relation is none %}
        {% do run_query('create table ' ~ target_relation ~ ' as (' ~ sql ~ ') with no data') %}
        {% do run_query('alter table ' ~ target_relation ~ ' add column trip_key bigint generated always as identity') %}
        {% do run_query('alter table ' ~ target_relation ~ ' add constraint fact_taxi_trip_pkey primary key (trip_key)') %}
        {% do run_query('alter table ' ~ target_relation ~ ' alter column source_record_key set not null') %}
        {% do run_query('alter table ' ~ target_relation ~ ' add constraint fact_taxi_trip_source_record_key_key unique (source_record_key)') %}
        {% for required_column in [
            'raw_exact_fingerprint', 'candidate_trip_fingerprint',
            'pickup_date_key', 'dropoff_date_key', 'pickup_time_key', 'dropoff_time_key',
            'pickup_zone_key', 'dropoff_zone_key', 'vendor_key', 'payment_type_key',
            'rate_code_key', 'trip_count', 'file_version_id', 'is_revenue_kpi_eligible',
            'is_tip_kpi_eligible', 'is_speed_kpi_eligible',
            'is_duration_kpi_eligible', 'is_passenger_kpi_eligible'
        ] %}
            {% do run_query('alter table ' ~ target_relation ~ ' alter column ' ~ adapter.quote(required_column) ~ ' set not null') %}
        {% endfor %}
    {% endif %}

    {% set insert_columns = [] %}
    {% set target_column_names = [] %}
    {% set update_assignments = [] %}
    {% set source_insert_values = [] %}
    {% set current_values = [] %}
    {% set source_values = [] %}
    {% set excluded_values = [] %}
    {% for column in adapter.get_columns_in_relation(target_relation) %}
        {% if column.name | lower != 'trip_key' %}
            {% do insert_columns.append(adapter.quote(column.name)) %}
            {% do source_insert_values.append('source_rows.' ~ adapter.quote(column.name)) %}
            {% do target_column_names.append(column.name | lower) %}
            {% if column.name | lower != 'source_record_key' %}
                {% set quoted = adapter.quote(column.name) %}
                {% do update_assignments.append(quoted ~ ' = excluded.' ~ quoted) %}
                {% do current_values.append('current_row.' ~ quoted) %}
                {% do source_values.append('source_rows.' ~ quoted) %}
                {% do excluded_values.append('excluded.' ~ quoted) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% if insert_columns | length == 0 %}
        {{ exceptions.raise_compiler_error('fact_taxi_trip has no insertable columns') }}
    {% endif %}
    {% if existing_relation is not none %}
        {% set model_probe = run_query('select * from (' ~ sql ~ ') as source_rows limit 0') %}
        {% set model_column_names = [] %}
        {% for name in model_probe.column_names %}
            {% do model_column_names.append(name | lower) %}
        {% endfor %}
        {% if model_column_names | sort != target_column_names | sort %}
            {{ exceptions.raise_compiler_error('fact_taxi_trip model columns differ from the identity table; use a reviewed migration') }}
        {% endif %}
    {% endif %}

    {% if lifecycle_mode == 'archive' %}
        {% set temp_source = adapter.quote('fact_identity_source_rows') %}
        {% set temp_stale = adapter.quote('fact_identity_stale_keys') %}
        {% set temp_new = adapter.quote('fact_identity_new_keys') %}

        {% do run_query('create schema if not exists ' ~ adapter.quote(history_schema)) %}
        {% do run_query(
            'create table if not exists ' ~ history_relation ~ ' (' ~
            'event_id bigint generated always as identity primary key, ' ~
            'source_record_key char(64) not null, trip_key bigint, ' ~
            'from_disposition text, to_disposition text not null, ' ~
            'dq_rule_ids text[] not null, change_type text not null, ' ~
            'batch_id bigint, changed_at timestamptz not null default clock_timestamp(), ' ~
            'prior_fact jsonb)'
        ) %}
        {# A never-accepted reject has no trip_key yet. #}
        {% do run_query('alter table ' ~ history_relation ~ ' alter column trip_key drop not null') %}
        {% do run_query('create index if not exists ' ~
            adapter.quote('fact_taxi_trip_history_source_event_idx') ~
            ' on ' ~ history_relation ~ ' (source_record_key, event_id desc)') %}
        {% if existing_relation is not none %}
            {# One-time migration of already materialized Fact identities. #}
            {% do run_query(
                'insert into ' ~ history_relation ~
                ' (source_record_key, trip_key, from_disposition, to_disposition, ' ~
                'dq_rule_ids, change_type, batch_id, prior_fact) ' ~
                'select f.source_record_key, f.trip_key, ' ~
                "'PREEXISTING', f.primary_disposition, f.dq_rule_ids, " ~
                "'MIGRATE', f.batch_id, to_jsonb(f) " ~
                'from ' ~ target_relation ~ ' f where not exists (' ~
                'select 1 from ' ~ history_relation ~
                ' h where h.source_record_key = f.source_record_key)'
            ) %}
        {% endif %}
        {% do run_query('create temporary table ' ~ temp_source ~
            ' on commit drop as select * from (' ~ sql ~ ') as source_rows') %}
        {% do run_query(
            'create temporary table ' ~ temp_stale ~ ' on commit drop as ' ~
            'select f.source_record_key, c.primary_disposition as new_disposition, ' ~
            'c.dq_rule_ids as new_rule_ids, coalesce(c.batch_id, f.batch_id) as batch_id, ' ~
            '(r.source_record_key is not null) as has_raw, ' ~
            '(c.source_record_key is not null or t.source_record_key is not null) as has_branch ' ~
            'from ' ~ target_relation ~ ' f ' ~
            'left join ' ~ ref('int_taxi_trip_accepted') ~ ' a on a.source_record_key = f.source_record_key ' ~
            'left join ' ~ ref('int_taxi_trip_classified') ~ ' c on c.source_record_key = f.source_record_key ' ~
            'left join ' ~ source('mage_dq', 'technical_quarantine') ~
            ' t on t.source_record_key = f.source_record_key ' ~
            'left join ' ~ source('stage', 'yellow_taxi_raw') ~
            ' r on r.source_record_key = f.source_record_key ' ~
            'where f.file_version_id in (select distinct file_version_id from ' ~
            ref('int_taxi_trip_classified') ~ ') and a.source_record_key is null'
        ) %}
        {% set untraceable = run_query('select source_record_key from ' ~ temp_stale ~
            ' where not has_raw or not has_branch limit 1') %}
        {% if untraceable.rows | length > 0 %}
            {{ exceptions.raise_compiler_error(
                'Refusing Fact deactivation without raw and quarantine lineage for source key: ' ~
                untraceable.rows[0][0]
            ) }}
        {% endif %}

        {% do run_query(
            'insert into ' ~ history_relation ~
            ' (source_record_key, trip_key, from_disposition, to_disposition, ' ~
            'dq_rule_ids, change_type, batch_id, prior_fact) ' ~
            'select f.source_record_key, f.trip_key, f.primary_disposition, ' ~
            "coalesce(s.new_disposition, 'TECHNICAL_REJECT'), " ~
            "coalesce(s.new_rule_ids, array['TECHNICAL_REJECT']::text[]), " ~
            "'DEACTIVATE', s.batch_id, to_jsonb(f) " ~
            'from ' ~ target_relation ~ ' f join ' ~ temp_stale ~
            ' s on s.source_record_key = f.source_record_key'
        ) %}
        {% if var('fact_lifecycle_fail_after_archive', false) %}
            {% if not var('fixture_mode', false) %}
                {{ exceptions.raise_compiler_error('Fault injection requires fixture_mode') }}
            {% endif %}
            {% do run_query('select 1 / 0') %}
        {% endif %}
        {% do run_query('delete from ' ~ target_relation ~ ' f using ' ~ temp_stale ~
            ' s where s.source_record_key = f.source_record_key') %}

        {% do run_query('create temporary table ' ~ temp_new ~ ' on commit drop as ' ~
            'select source_rows.source_record_key from ' ~ temp_source ~ ' source_rows ' ~
            'left join ' ~ target_relation ~ ' f on f.source_record_key = source_rows.source_record_key ' ~
            'where f.source_record_key is null') %}

        {# Archive the previous Fact image before an in-place update. #}
        {% do run_query(
            'insert into ' ~ history_relation ~
            ' (source_record_key, trip_key, from_disposition, to_disposition, ' ~
            'dq_rule_ids, change_type, batch_id, prior_fact) ' ~
            'select f.source_record_key, f.trip_key, f.primary_disposition, ' ~
            'source_rows.primary_disposition, source_rows.dq_rule_ids, ' ~
            "case when f.primary_disposition is distinct from source_rows.primary_disposition " ~
            "then 'STATUS_CHANGE' else 'DATA_UPDATE' end, " ~
            'source_rows.batch_id, to_jsonb(f) ' ~
            'from ' ~ target_relation ~ ' f join ' ~ temp_source ~
            ' source_rows on source_rows.source_record_key = f.source_record_key ' ~
            'where row(' ~ (current_values | join(', ') | replace('current_row.', 'f.')) ~
            ') is distinct from row(' ~ (source_values | join(', ')) ~ ')'
        ) %}

        {# Restore an old identity for a previously deactivated source key. #}
        {% do run_query(
            'insert into ' ~ target_relation ~
            ' (trip_key, ' ~ (insert_columns | join(', ')) ~ ') overriding system value ' ~
            'select prior.trip_key, ' ~ (source_insert_values | join(', ')) ~
            ' from ' ~ temp_source ~ ' source_rows join ' ~ temp_new ~
            ' n on n.source_record_key = source_rows.source_record_key ' ~
            'join lateral (select h.trip_key from ' ~ history_relation ~
            ' h where h.source_record_key = source_rows.source_record_key ' ~
            'and h.trip_key is not null ' ~
            'order by h.event_id desc limit 1) prior on true ' ~
            'on conflict (source_record_key) do nothing'
        ) %}
    {% endif %}

    {% call statement('main') %}
        insert into {{ target_relation }} as current_row ({{ insert_columns | join(', ') }})
        select {{ insert_columns | join(', ') }}
        from {% if lifecycle_mode == 'archive' %}{{ temp_source }}{% else %}({{ sql }}){% endif %} as source_rows
        where not exists (
            select 1 from {{ target_relation }} existing
            where existing.source_record_key = source_rows.source_record_key
              and row({{ current_values | join(', ') | replace('current_row.', 'existing.') }})
                  is not distinct from row({{ source_values | join(', ') }})
        )
        on conflict (source_record_key) do update
        set {{ update_assignments | join(', ') }}
        where row({{ current_values | join(', ') }})
            is distinct from row({{ excluded_values | join(', ') }})
    {% endcall %}

    {% if lifecycle_mode == 'archive' %}
        {% do run_query(
            'with previous as (select distinct on (h.source_record_key) ' ~
            'h.source_record_key, h.to_disposition from ' ~ history_relation ~
            ' h join ' ~ temp_new ~ ' n on n.source_record_key = h.source_record_key ' ~
            'order by h.source_record_key, h.event_id desc) ' ~
            'insert into ' ~ history_relation ~
            ' (source_record_key, trip_key, from_disposition, to_disposition, ' ~
            'dq_rule_ids, change_type, batch_id, prior_fact) ' ~
            'select f.source_record_key, f.trip_key, ' ~
            "coalesce(p.to_disposition, 'NEW'), f.primary_disposition, " ~
            "f.dq_rule_ids, 'ACTIVATE', f.batch_id, null::jsonb " ~
            'from ' ~ temp_new ~ ' n join ' ~ target_relation ~
            ' f on f.source_record_key = n.source_record_key ' ~
            'left join previous p on p.source_record_key = n.source_record_key'
        ) %}
        {# Record new/current rejects once, including rows that never entered Fact. #}
        {% do run_query(
            'with rejected as (' ~
            'select source_record_key, primary_disposition, dq_rule_ids, batch_id ' ~
            'from ' ~ ref('int_taxi_trip_classified') ~
            " where primary_disposition in ('BUSINESS_QUARANTINE', 'DUPLICATE') " ~
            'union all select source_record_key, ' ~
            "'TECHNICAL_REJECT'::text, array['TECHNICAL_REJECT']::text[], null::bigint " ~
            'from ' ~ source('mage_dq', 'technical_quarantine') ~
            '), previous as (select distinct on (h.source_record_key) ' ~
            'h.source_record_key, h.trip_key, h.to_disposition from ' ~ history_relation ~
            ' h order by h.source_record_key, h.event_id desc) ' ~
            'insert into ' ~ history_relation ~
            ' (source_record_key, trip_key, from_disposition, to_disposition, ' ~
            'dq_rule_ids, change_type, batch_id, prior_fact) ' ~
            'select r.source_record_key, p.trip_key, ' ~
            "coalesce(p.to_disposition, 'NEW'), r.primary_disposition, " ~
            "r.dq_rule_ids, 'REJECTED', r.batch_id, null::jsonb " ~
            'from rejected r left join previous p on p.source_record_key = r.source_record_key ' ~
            'where p.to_disposition is distinct from r.primary_disposition'
        ) %}
    {% endif %}

    {% do persist_docs(target_relation, model) %}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {% do adapter.commit() %}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
