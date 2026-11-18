select c.source_record_key
from {{ ref('stg_yellow_taxi') }} c
left join {{ source('stage', 'yellow_taxi_raw') }} r
    on r.source_record_key = c.source_record_key
where trim(c.source_record_key) <>
      encode(digest(lower(trim(c.source_file_sha256)) || ':' || c.source_row_number::text,
                    'sha256'), 'hex')
   or r.source_record_key is null
union all
select t.source_record_key
from {{ source('mage_dq', 'technical_quarantine') }} t
left join {{ source('stage', 'yellow_taxi_raw') }} r
    on r.source_record_key = t.source_record_key
left join {{ source('stage', 'yellow_taxi_canonical') }} c
    on c.source_record_key = t.source_record_key
where r.source_record_key is null or c.source_record_key is not null
