{% macro minimal_create_fact_version_guard() %}
create or replace function pg_temp.minimal_fact_version_guard(
    current_version bigint, incoming_version bigint
) returns bigint language plpgsql immutable parallel safe cost 1 as $minimal_fact_version_guard$
begin
    if current_version is distinct from incoming_version then
        raise exception 'Source identity collision across file versions; refusing to overwrite unrelated Fact';
    end if;
    return incoming_version;
end;
$minimal_fact_version_guard$;
{% endmacro %}
