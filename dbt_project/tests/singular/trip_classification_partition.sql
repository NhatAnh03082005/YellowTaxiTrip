-- Every canonical source key must be in exactly one final Phase 2 branch.
with current_file_versions as (
    select distinct file_version_id from {{ ref('stg_yellow_taxi') }}
), branch_keys as (
    select source_record_key from {{ ref('int_taxi_trip_accepted') }}
    union all
    select source_record_key from {{ ref('business_quarantine') }}
    where file_version_id in (select file_version_id from current_file_versions)
    union all
    select source_record_key from {{ ref('duplicate_quarantine') }}
    where file_version_id in (select file_version_id from current_file_versions)
), counts as (
    select source_record_key, count(*) as branch_count
    from branch_keys group by source_record_key
)
select c.source_record_key
from {{ ref('stg_yellow_taxi') }} c
left join counts b on b.source_record_key = c.source_record_key
where coalesce(b.branch_count, 0) <> 1
union all
select b.source_record_key
from counts b
left join {{ ref('stg_yellow_taxi') }} c on c.source_record_key = b.source_record_key
where c.source_record_key is null
