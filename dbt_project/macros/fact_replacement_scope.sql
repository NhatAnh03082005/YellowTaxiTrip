{% macro fact_replacement_scope(batch_id) %}
    select binding.old_version_id, binding.new_version_id
    from {{ source('control', 'etl_replacement') }} binding
    join {{ source('control', 'etl_file') }} attempt on attempt.batch_id = binding.batch_id
        and attempt.file_version_id = binding.new_version_id
    join {{ source('control', 'etl_batch') }} current_batch on current_batch.batch_id = binding.batch_id
    where binding.batch_id = {{ batch_id | int }}
      and attempt.file_status = 'STAGED'
      and current_batch.status in ('STAGED', 'DBT_RUNNING', 'RECONCILING')
      and exists (
          select 1 from {{ source('stage', 'yellow_taxi_raw') }} r
          where r.batch_id = attempt.batch_id
            and r.file_version_id = attempt.file_version_id
            and r.source_file_sha256 = attempt.source_file_sha256
      )
{% endmacro %}
