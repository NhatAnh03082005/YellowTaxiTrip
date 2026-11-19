{% macro replacement_olap_scope(grain) %}
    {% set batch_id = var('load_batch_id', none) %}
    {% if batch_id is none %}
        {{ exceptions.raise_compiler_error('Replacement OLAP requires load_batch_id') }}
    {% endif %}
    {% if grain == 'pickup_date_key' %}
        select pickup_date_key
        from {{ ref('fact_taxi_trip') }}
        where file_version_id in (
            select file_version_id from {{ source('stage', 'yellow_taxi_raw') }}
            where batch_id = {{ batch_id | int }}
        )
        union
        select (prior_fact->>'pickup_date_key')::integer
        from {{ source('fact_lifecycle', 'history') }}
        where batch_id = {{ batch_id | int }} and change_type = 'REPLACE_VERSION'
    {% elif grain == 'source_year_month' %}
        select source_year_month from {{ source('stage', 'yellow_taxi_raw') }}
        where batch_id = {{ batch_id | int }}
        union
        select prior_fact->>'source_year_month'
        from {{ source('fact_lifecycle', 'history') }}
        where batch_id = {{ batch_id | int }} and change_type = 'REPLACE_VERSION'
    {% else %}
        {{ exceptions.raise_compiler_error('Unsupported replacement OLAP grain: ' ~ grain) }}
    {% endif %}
{% endmacro %}

{% macro invalidate_replacement_olap(grain) %}
    {% if var('apply_approved_fact_replacement', false) and is_incremental() %}
        delete from {{ this }}
        where {{ adapter.quote(grain) }} in ({{ replacement_olap_scope(grain) }})
    {% endif %}
{% endmacro %}
