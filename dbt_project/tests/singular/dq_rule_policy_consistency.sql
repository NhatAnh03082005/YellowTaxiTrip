{{ config(tags=['minimal_batch']) }}
select rule_id
from {{ ref('dq_rule_policy') }}
where (severity = 'INFO' and (severity_rank <> 1 or disposition_action <> 'INFO'))
   or (severity = 'WARNING' and (severity_rank <> 2 or disposition_action <> 'WARNING'))
   or (severity = 'HIGH' and (severity_rank <> 3
       or disposition_action not in ('BUSINESS_QUARANTINE', 'DUPLICATE')))
