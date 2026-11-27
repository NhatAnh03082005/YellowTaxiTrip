{{ config(tags=['minimal_batch']) }}
select 1
where not exists (
    select 1
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    join pg_attribute a on a.attrelid = c.oid and a.attname = 'trip_key'
    where n.nspname = '{{ ref('fact_taxi_trip').schema }}'
      and c.relname = '{{ ref('fact_taxi_trip').identifier }}'
      and c.relkind = 'r'
      and a.attidentity = 'a'
)
or not exists (
    select 1
    from pg_constraint k
    join pg_class c on c.oid = k.conrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = '{{ ref('fact_taxi_trip').schema }}'
      and c.relname = '{{ ref('fact_taxi_trip').identifier }}'
      and k.contype = 'u'
      and k.conname = 'fact_taxi_trip_source_record_key_key'
)
