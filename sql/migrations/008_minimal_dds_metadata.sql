\set ON_ERROR_STOP on

-- Additive DDS metadata cutover. Apply only after 005, 006 and 007, then
-- install the current 003_control_functions.sql and 006 function/view bodies.
-- No Fact/history table is dropped or rebuilt here. A legacy Fact with a
-- different physical shape requires a separate, reviewed side-by-side target.
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('yellow_taxi_backfill_runner'));

DO $$
BEGIN
    IF to_regclass('control.file_reconciliation_history') IS NULL
       OR to_regclass('control.etl_step_run') IS NULL
       OR to_regclass('control.etl_backfill_run') IS NULL THEN
        RAISE EXCEPTION 'Apply control base schema and migrations 005-007 before 008';
    END IF;
    IF to_regclass('dds.fact_taxi_trip') IS NOT NULL AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='dds' AND table_name='fact_taxi_trip'
          AND column_name IN ('raw_exact_fingerprint','candidate_trip_fingerprint')
    ) THEN
        RAISE EXCEPTION 'Legacy Fact shape detected. Build and validate minimal DDS in a separate target; do not alter populated Fact in place';
    END IF;
END
$$;

ALTER TABLE control.file_reconciliation_history
    ADD COLUMN IF NOT EXISTS measure_details jsonb NULL;

-- Historical rows without complete scoped measures remain NULL, never fake 0.
UPDATE control.file_reconciliation_history h
SET measure_details = s.details
FROM control.etl_step_run s
WHERE s.batch_id = h.successful_batch_id
  AND s.step_name = 'RECONCILIATION'
  AND s.status = 'SUCCESS'
  AND s.details ? 'raw_fare_amount'
  AND h.measure_details IS NULL;

CREATE OR REPLACE VIEW control.v_file_reconciliation_minimal AS
SELECT h.file_version_id, h.successful_batch_id, h.source_rows,
       h.canonical_rows, h.technical_rows, h.fact_rows,
       h.business_rows, h.duplicate_rows, h.source_row_delta,
       h.trusted_row_delta, h.source_amount_delta AS source_total_delta,
       h.trusted_amount_delta AS trusted_total_delta,
       (h.measure_details->>'source_fare_delta')::numeric AS source_fare_delta,
       (h.measure_details->>'trusted_fare_delta')::numeric AS trusted_fare_delta,
       (h.measure_details->>'source_tip_delta')::numeric AS source_tip_delta,
       (h.measure_details->>'trusted_tip_delta')::numeric AS trusted_tip_delta,
       h.measure_details IS NOT NULL AS has_full_measure_evidence,
       h.recorded_at
FROM control.file_reconciliation_history h;

COMMENT ON COLUMN control.file_reconciliation_history.measure_details IS
    'Scoped fare/tip/total eligible counts, sums and deltas from a successful DDS batch; NULL for legacy rows without evidence.';

COMMIT;
