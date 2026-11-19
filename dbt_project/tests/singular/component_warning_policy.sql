{{ config(enabled=not var('dbt_minimal_mode', false)) }}
select source_record_key
from {{ ref('int_taxi_trip_classified') }}
where (component_reconciliation_status = 'MATCHED'
       and dq_rule_ids && array['AMT-006', 'AMT-007', 'AMT-008']::text[])
   or (component_reconciliation_status = 'ROUNDING_WARNING'
       and not 'AMT-006' = any(dq_rule_ids))
   or (component_reconciliation_status = 'REVIEW_WARNING'
       and not 'AMT-007' = any(dq_rule_ids))
   or (component_reconciliation_status = 'NOT_RECONCILABLE'
       and not 'AMT-008' = any(dq_rule_ids))
   or ((amt_006 or amt_007 or amt_008)
       and not dup_001 and not has_business_quarantine_rule
       and primary_disposition <> 'WARNING')
