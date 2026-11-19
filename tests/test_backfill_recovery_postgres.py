"""Rollback-only PostgreSQL fixture for downstream recovery gates."""
from pathlib import Path
import subprocess
import unittest


class BackfillRecoveryPostgres(unittest.TestCase):
    def run_fixture(self, body):
        root = Path(__file__).resolve().parents[1]
        migration = '\n'.join(
            line for line in (root / 'sql/migrations/006_backfill_recovery_and_cleanup.sql')
            .read_text(encoding='utf-8').splitlines()
            if not line.lstrip().startswith('\\')
        )
        result = subprocess.run(
            ['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
            cwd=root, input=('BEGIN;\n' + migration + '\n' + body + '\nROLLBACK;\n'),
            text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_resume_requires_single_preserved_batch(self):
        root = Path(__file__).resolve().parents[1]
        migration = '\n'.join(
            line for line in (root / 'sql/migrations/006_backfill_recovery_and_cleanup.sql')
            .read_text(encoding='utf-8').splitlines()
            if not line.lstrip().startswith('\\')
        )
        fixture = r"""
BEGIN;
__MIGRATION__

DO $$
DECLARE
    v_batch bigint;
    v_other bigint;
    v_version bigint;
    v_sha text := repeat('7', 64);
    v_schema text := repeat('8', 64);
BEGIN
    INSERT INTO control.source_file_version (
        source_file, source_year_month, source_file_sha256, schema_fingerprint,
        schema_version, content_length
    ) VALUES ('fixture-recovery.parquet', '2098-01', v_sha, v_schema, 'yt_schema_v002', 1)
    RETURNING file_version_id INTO v_version;

    INSERT INTO control.etl_batch (
        pipeline_name, source_name, run_type, triggered_by, lset, cet,
        status, discovery_status, started_at, ended_at, catalog_expected_count,
        catalog_checked_count, files_selected, files_discovered, files_validated,
        error_code, error_message
    ) VALUES (
        'fixture_recovery', 'yellow_taxi_tripdata', 'TEST', 'unittest', now(), now(),
        'FAILED', 'SUCCESS', now() - interval '1 second', now(), 1, 1, 1, 1, 1,
        'DOWNSTREAM_FAILED', 'fixture failure'
    ) RETURNING batch_id INTO v_batch;

    INSERT INTO control.etl_file (
        batch_id, source_file, source_year_month, content_length,
        source_file_sha256, schema_fingerprint, schema_version, file_version_id,
        file_status, rows_read, rows_staged
    ) VALUES (
        v_batch, 'fixture-recovery.parquet', '2098-01', 1,
        v_sha, v_schema, 'yt_schema_v002', v_version, 'STAGED', 1, 1
    );
    INSERT INTO control.etl_step_run (batch_id, step_name, status, started_at, ended_at)
    SELECT v_batch, step_name, 'FAILED', now(), now()
    FROM unnest(ARRAY['DBT_BUILD_DDS','DBT_TEST','RECONCILIATION','DBT_BUILD_OLAP']) step_name;

    INSERT INTO stage.yellow_taxi_raw (
        batch_id, source_file, source_year_month, source_file_sha256,
        source_row_number, source_record_key, schema_fingerprint, schema_version,
        file_version_id
    ) VALUES (v_batch, 'fixture-recovery.parquet', '2098-01', v_sha,
              1, repeat('9',64), v_schema, 'yt_schema_v002', v_version);
    INSERT INTO stage.yellow_taxi_canonical (
        batch_id, source_file, source_year_month, source_file_sha256,
        source_row_number, source_record_key, schema_fingerprint, schema_version,
        file_version_id
    ) VALUES (v_batch, 'fixture-recovery.parquet', '2098-01', v_sha,
              1, repeat('9',64), v_schema, 'yt_schema_v002', v_version);

    PERFORM control.resume_failed_downstream(v_batch);
    IF (SELECT status FROM control.etl_batch WHERE batch_id = v_batch) <> 'DBT_RUNNING'
       OR EXISTS (SELECT 1 FROM control.etl_step_run WHERE batch_id = v_batch AND status <> 'PENDING') THEN
        RAISE EXCEPTION 'valid downstream resume did not reset state';
    END IF;

    INSERT INTO control.etl_batch (
        pipeline_name, source_name, run_type, triggered_by, lset, cet,
        status, discovery_status, catalog_expected_count, catalog_checked_count
    ) VALUES ('fixture_other', 'yellow_taxi_tripdata', 'TEST', 'unittest', now(), now(),
              'RUNNING', 'SUCCESS', 1, 1)
    RETURNING batch_id INTO v_other;
    INSERT INTO stage.yellow_taxi_raw (
        batch_id, source_file, source_year_month, source_file_sha256,
        source_row_number, source_record_key, schema_fingerprint, schema_version,
        file_version_id
    ) VALUES (v_other, 'fixture-other.parquet', '2098-02', repeat('a',64),
              1, repeat('b',64), v_schema, 'yt_schema_v002', v_version);

    UPDATE control.etl_batch SET status = 'FAILED', ended_at = now(),
        error_code = 'DOWNSTREAM_FAILED' WHERE batch_id = v_batch;
    BEGIN
        PERFORM control.resume_failed_downstream(v_batch);
        RAISE EXCEPTION 'mixed staging was accepted';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'mixed staging was accepted' THEN RAISE; END IF;
    END;
END
$$;
ROLLBACK;
""".replace('__MIGRATION__', migration)
        result = subprocess.run(
            ['docker', 'compose', 'exec', '-T', 'postgres', 'sh', '-c',
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
            cwd=root, input=fixture, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_global_view_requires_all_24_balanced_children(self):
        self.run_fixture(r"""
DO $$
DECLARE
    v_run bigint;
    v_batch bigint;
    v_version bigint;
    i integer;
    v_month text;
BEGIN
    INSERT INTO control.etl_backfill_run (
        pipeline_name, source_name, source_month_from, source_month_to,
        expected_file_count, completed_file_count, dds_validation_status,
        olap_bi_validation_status
    ) VALUES ('fixture_global', 'yellow_taxi_tripdata', '2096-01', '2097-12',
              24, 24, 'SUCCESS', 'SUCCESS')
    RETURNING backfill_run_id INTO v_run;

    FOR i IN 1..24 LOOP
        v_month := CASE WHEN i <= 12 THEN '2096-' ELSE '2097-' END
                   || lpad((((i - 1) % 12) + 1)::text, 2, '0');
        INSERT INTO control.source_file_version (
            source_file, source_year_month, source_file_sha256,
            schema_fingerprint, schema_version, content_length
        ) VALUES ('fixture-global-' || i || '.parquet', v_month,
                  lpad(to_hex(1000 + i), 64, '0'), repeat('c',64),
                  'yt_schema_v002', 1)
        RETURNING file_version_id INTO v_version;
        INSERT INTO control.etl_batch (
            pipeline_name, source_name, run_type, triggered_by, lset, cet,
            status, discovery_status, started_at, ended_at,
            catalog_expected_count, catalog_checked_count, files_selected,
            files_discovered, files_validated
        ) VALUES ('fixture_global', 'yellow_taxi_tripdata', 'TEST', 'unittest',
                  now(), now(), 'SUCCESS', 'SUCCESS', now() - interval '1 second', now(),
                  1, 1, 1, 1, 1)
        RETURNING batch_id INTO v_batch;
        INSERT INTO control.etl_step_run (
            batch_id, step_name, status, started_at, ended_at
        ) SELECT v_batch, step_name, 'SUCCESS', now(), now()
          FROM unnest(ARRAY['DISCOVERY','FILE_VALIDATION','STAGING','DBT_BUILD_DDS','DBT_TEST','RECONCILIATION','DBT_BUILD_OLAP']) step_name;
        INSERT INTO control.file_reconciliation_history (
            file_version_id, successful_batch_id, source_rows, canonical_rows,
            technical_rows, fact_rows, business_rows, duplicate_rows,
            source_amount_eligible_rows, canonical_amount_eligible_rows,
            technical_amount_eligible_rows, fact_amount_eligible_rows,
            business_amount_eligible_rows, duplicate_amount_eligible_rows,
            source_amount, canonical_amount, technical_amount, fact_amount,
            business_amount, duplicate_amount, source_row_delta, trusted_row_delta,
            source_amount_delta, trusted_amount_delta
        ) VALUES (v_version, v_batch, 10, 9, 1, 7, 1, 1,
                  10, 9, 1, 7, 1, 1, 100, 90, 10, 70, 10, 10, 0, 0, 0, 0);
        INSERT INTO control.etl_backfill_file (
            backfill_run_id, source_year_month, file_version_id, batch_id,
            status, completed_at, cleanup_status, cleaned_at
        ) VALUES (v_run, v_month, v_version, v_batch, 'SUCCESS', now(), 'SUCCESS', now());
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM control.v_backfill_global_reconciliation
        WHERE backfill_run_id = v_run AND validated_file_count = 24
          AND successful_child_count = 24 AND cleanup_success_count = 24
          AND required_step_failure_count = 0 AND is_balanced
          AND source_rows = 240 AND fact_rows = 168
    ) THEN RAISE EXCEPTION '24-child global gate mismatch'; END IF;
END
$$;
""")

    def test_replay_authorization_is_exact_and_audited(self):
        self.run_fixture(r"""
DO $$
DECLARE
    v_success bigint;
    v_replay bigint;
    v_version bigint;
    v_file bigint;
BEGIN
    INSERT INTO control.source_file_version (
        source_file, source_year_month, source_file_sha256,
        schema_fingerprint, schema_version, content_length
    ) VALUES ('fixture-replay.parquet','2094-01',repeat('d',64),repeat('e',64),
              'yt_schema_v002',1) RETURNING file_version_id INTO v_version;
    INSERT INTO control.etl_batch (
        pipeline_name, source_name, run_type, triggered_by, lset, cet,
        status, discovery_status, started_at, ended_at
    ) VALUES ('fixture_replay','yellow_taxi_tripdata','TEST','unittest',now(),now(),
              'SUCCESS','SUCCESS',now()-interval '1 second',now())
    RETURNING batch_id INTO v_success;
    INSERT INTO control.file_reconciliation_history (
        file_version_id, successful_batch_id, source_rows, canonical_rows,
        technical_rows, fact_rows, business_rows, duplicate_rows,
        source_amount_eligible_rows, canonical_amount_eligible_rows,
        technical_amount_eligible_rows, fact_amount_eligible_rows,
        business_amount_eligible_rows, duplicate_amount_eligible_rows,
        source_amount, canonical_amount, technical_amount, fact_amount,
        business_amount, duplicate_amount, source_row_delta, trusted_row_delta,
        source_amount_delta, trusted_amount_delta
    ) VALUES (v_version,v_success,1,1,0,1,0,0,1,1,0,1,0,0,10,10,0,10,0,0,0,0,0,0);
    INSERT INTO control.etl_batch (
        pipeline_name, source_name, run_type, triggered_by, lset, cet,
        status, discovery_status
    ) VALUES ('fixture_replay','yellow_taxi_tripdata','TEST','unittest',now(),now(),
              'RUNNING','SUCCESS') RETURNING batch_id INTO v_replay;
    INSERT INTO control.etl_file (
        batch_id, source_file, source_year_month, content_length,
        source_file_sha256, schema_fingerprint, schema_version,
        file_version_id, file_status
    ) VALUES (v_replay,'fixture-replay.parquet','2094-01',1,repeat('d',64),
              repeat('e',64),'yt_schema_v002',v_version,'SKIPPED_ALREADY_PROCESSED')
    RETURNING etl_file_id INTO v_file;
    PERFORM control.authorize_file_replay(v_file,v_version,'qa_owner','FIN-09 rollback fixture');
    IF (SELECT file_status FROM control.etl_file WHERE etl_file_id=v_file) <> 'VALIDATED'
       OR NOT EXISTS (SELECT 1 FROM control.etl_log WHERE etl_file_id=v_file
                      AND event_code='FILE_REPLAY_AUTHORIZED')
    THEN RAISE EXCEPTION 'replay authorization was not exact and audited'; END IF;
END
$$;
""")


if __name__ == '__main__':
    unittest.main()
