\set ON_ERROR_STOP on
-- Apply only after review. No data deletion/reload. Mage fails closed on raw
-- cast errors until this nullable column exists; healthy files remain compatible.
BEGIN;
ALTER TABLE stage.yellow_taxi_raw ADD COLUMN IF NOT EXISTS raw_payload jsonb NULL;
COMMENT ON COLUMN stage.yellow_taxi_raw.raw_payload IS
  'Original source fields for rows not representable in typed Raw. NULL for ordinary rows. Consumed by dbt raw fingerprint fallback.';
COMMIT;
