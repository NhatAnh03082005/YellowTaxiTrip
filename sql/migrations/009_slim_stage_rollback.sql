\set ON_ERROR_STOP on
-- Manual rollback for migration 009 before the slim tables receive data.
-- If slim staging contains rows, export/reconcile them first; this script fails closed.
BEGIN;
DO $guard$
DECLARE
  dependent_views text;
BEGIN
  IF to_regclass('stage.stage_storage_contract') IS NULL THEN
    RAISE EXCEPTION 'Slim staging is not installed';
  END IF;
  IF EXISTS(SELECT 1 FROM stage.yellow_taxi_raw_data)
     OR EXISTS(SELECT 1 FROM stage.yellow_taxi_canonical_data)
     OR EXISTS(SELECT 1 FROM stage.stage_file_batch) THEN
    RAISE EXCEPTION 'Slim staging contains data; reconcile/export it before manual rollback';
  END IF;
  IF EXISTS(SELECT 1 FROM control.etl_batch WHERE status NOT IN ('SUCCESS','FAILED','CANCELLED')) THEN
    RAISE EXCEPTION 'An active/pending batch exists; refusing rollback';
  END IF;
  SELECT string_agg(format('%I.%I',dns.nspname,dv.relname),', ' ORDER BY dns.nspname,dv.relname)
    INTO dependent_views
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid=d.objid
    JOIN pg_class dv ON dv.oid=r.ev_class AND dv.relkind IN ('v','m')
    JOIN pg_namespace dns ON dns.oid=dv.relnamespace
    JOIN pg_class src ON src.oid=d.refobjid
    JOIN pg_namespace sns ON sns.oid=src.relnamespace
   WHERE sns.nspname='stage'
     AND src.relname IN ('yellow_taxi_raw','yellow_taxi_canonical')
     AND dv.oid<>src.oid;
  IF dependent_views IS NOT NULL THEN
    RAISE EXCEPTION 'Dependent views must be reviewed/dropped before rollback: %',dependent_views;
  END IF;
END;
$guard$;

DROP VIEW stage.yellow_taxi_raw;
DROP VIEW stage.yellow_taxi_canonical;
DROP TABLE stage.yellow_taxi_raw_data;
DROP TABLE stage.yellow_taxi_canonical_data;
DROP TABLE stage.stage_file_batch;
DROP TABLE stage.stage_storage_contract;
ALTER TABLE stage.yellow_taxi_raw_wide_backup RENAME TO yellow_taxi_raw;
ALTER TABLE stage.yellow_taxi_canonical_wide_backup RENAME TO yellow_taxi_canonical;
DROP TRIGGER guard_slim_file_metadata ON control.source_file_version;
DROP FUNCTION control.guard_slim_file_metadata();
ALTER TABLE control.source_file_version DROP CONSTRAINT ck_mage_stage_schema_version;
COMMIT;
