\set ON_ERROR_STOP on
-- Opt-in only after DDS + tests/reconciliation SUCCESS and staging cleanup.
-- Never apply automatically from Mage/runner. Keeps empty legacy tables as backups.
BEGIN;
DO $guard$
DECLARE
  dependent_views text;
BEGIN
  IF to_regclass('stage.stage_storage_contract') IS NOT NULL THEN
    RAISE EXCEPTION 'Slim staging already configured; do not reapply migration';
  END IF;
  IF EXISTS(SELECT 1 FROM stage.yellow_taxi_raw)
     OR EXISTS(SELECT 1 FROM stage.yellow_taxi_canonical) THEN
    RAISE EXCEPTION 'Staging is not empty; complete DDS and approved cleanup first';
  END IF;
  IF EXISTS(SELECT 1 FROM control.etl_batch WHERE status NOT IN ('SUCCESS','FAILED','CANCELLED')) THEN
    RAISE EXCEPTION 'An active/pending batch exists; refusing staging migration';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='stage' AND table_name='yellow_taxi_raw' AND column_name='raw_payload') THEN
    RAISE EXCEPTION 'Apply reviewed migration 008 before slim staging';
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
    RAISE EXCEPTION 'Dependent views must be reviewed/dropped and rebuilt after slim migration: %',dependent_views;
  END IF;
END;
$guard$;

-- Same supported schema rule, checked once at immutable file/version rather than millions of times.
ALTER TABLE control.source_file_version ADD CONSTRAINT ck_mage_stage_schema_version
  CHECK(schema_version IN ('yt_schema_v001','yt_schema_v002'));

CREATE TABLE stage.stage_file_batch (
  file_version_id bigint PRIMARY KEY REFERENCES control.source_file_version(file_version_id),
  batch_id bigint NOT NULL REFERENCES control.etl_batch(batch_id),
  source_last_modified timestamptz NULL
);

CREATE FUNCTION control.guard_slim_file_metadata() RETURNS trigger LANGUAGE plpgsql AS $immutable$
BEGIN
  IF ROW(NEW.source_file,NEW.source_year_month,NEW.source_file_sha256,NEW.schema_fingerprint,NEW.schema_version,NEW.source_last_modified)
     IS DISTINCT FROM ROW(OLD.source_file,OLD.source_year_month,OLD.source_file_sha256,OLD.schema_fingerprint,OLD.schema_version,OLD.source_last_modified) THEN
    RAISE EXCEPTION 'File version metadata is immutable; create a reviewed new version';
  END IF;
  RETURN NEW;
END;
$immutable$;
CREATE TRIGGER guard_slim_file_metadata BEFORE UPDATE ON control.source_file_version
  FOR EACH ROW EXECUTE FUNCTION control.guard_slim_file_metadata();

DO $slim$
DECLARE
  item record;
  columns_sql text;
BEGIN
  FOR item IN SELECT * FROM (VALUES
      ('yellow_taxi_raw','yellow_taxi_raw_data','yellow_taxi_raw_wide_backup'),
      ('yellow_taxi_canonical','yellow_taxi_canonical_data','yellow_taxi_canonical_wide_backup')
  ) t(logical_name,physical_name,backup_name)
  LOOP
    EXECUTE format('ALTER TABLE stage.%I RENAME TO %I',item.logical_name,item.backup_name);
    EXECUTE format('CREATE UNLOGGED TABLE stage.%I (LIKE stage.%I INCLUDING ALL)',item.physical_name,item.backup_name);
    EXECUTE format('ALTER TABLE stage.%I DROP COLUMN batch_id CASCADE, DROP COLUMN source_file CASCADE, DROP COLUMN source_year_month CASCADE,
      DROP COLUMN source_file_sha256 CASCADE, DROP COLUMN schema_fingerprint CASCADE,
      DROP COLUMN schema_version CASCADE, DROP COLUMN source_last_modified CASCADE',item.physical_name);
    -- LIKE copies CHECK/PK/unique/identity/indexes, but does not copy foreign keys.
    EXECUTE format('ALTER TABLE stage.%I ADD FOREIGN KEY(file_version_id) REFERENCES stage.stage_file_batch(file_version_id)',item.physical_name);
    SELECT string_agg(CASE WHEN a.attname IN ('source_file','source_year_month','source_file_sha256',
        'schema_fingerprint','schema_version')
        THEN format('v.%I',a.attname) WHEN a.attname IN ('batch_id','source_last_modified') THEN format('m.%I',a.attname) ELSE format('d.%I',a.attname) END,', ' ORDER BY a.attnum)
      INTO columns_sql
      FROM pg_attribute a WHERE a.attrelid=to_regclass('stage.'||item.backup_name)
        AND a.attnum>0 AND NOT a.attisdropped;
    EXECUTE format('CREATE VIEW stage.%I AS SELECT %s FROM stage.%I d
      JOIN stage.stage_file_batch m USING(file_version_id) JOIN control.source_file_version v USING(file_version_id)',item.logical_name,columns_sql,item.physical_name);
  END LOOP;
END;
$slim$;

CREATE TABLE stage.stage_storage_contract (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  storage_mode text NOT NULL CHECK(storage_mode='slim-v1'),
  installed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO stage.stage_storage_contract(singleton,storage_mode) VALUES(true,'slim-v1');
COMMENT ON VIEW stage.yellow_taxi_raw IS 'Read-compatible Raw; metadata joined losslessly from immutable file version. COPY uses yellow_taxi_raw_data with explicit slim mode.';
COMMENT ON VIEW stage.yellow_taxi_canonical IS 'Read-compatible Canonical; all measures/lineage retained. COPY uses yellow_taxi_canonical_data.';
COMMIT;
