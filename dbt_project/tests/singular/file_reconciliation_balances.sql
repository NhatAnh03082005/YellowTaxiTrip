select *
from {{ ref('file_reconciliation') }}
where source_file_sha256 is null
   or source_file is null
   or source_row_delta <> 0
   or trusted_row_delta <> 0
   or source_amount_eligible_row_delta <> 0
   or trusted_amount_eligible_row_delta <> 0
   or abs(source_amount_delta) > {{ var('amount_reconciliation_tolerance') }}
   or abs(trusted_amount_delta) > {{ var('amount_reconciliation_tolerance') }}
