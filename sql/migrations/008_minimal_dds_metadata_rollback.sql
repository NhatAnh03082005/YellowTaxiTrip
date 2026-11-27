\set ON_ERROR_STOP on

-- Non-destructive rollback of the compatibility view. Keep the additive
-- measure_details column and its audit evidence; reverting code must never
-- silently discard validated fare/tip counts or change a watermark.
BEGIN;
DROP VIEW IF EXISTS control.v_file_reconciliation_minimal;
COMMIT;
