BEGIN;

CREATE TABLE IF NOT EXISTS control.etl_replacement (
    batch_id bigint PRIMARY KEY REFERENCES control.etl_batch(batch_id),
    old_version_id bigint NOT NULL REFERENCES control.source_file_version(file_version_id),
    new_version_id bigint NOT NULL REFERENCES control.source_file_version(file_version_id),
    approval_file_id bigint NOT NULL REFERENCES control.etl_file(etl_file_id),
    bound_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (old_version_id <> new_version_id)
);

CREATE OR REPLACE FUNCTION control.bind_approved_replacement(p_batch_id bigint, p_approval_file_id bigint)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    current_attempt record;
    approval control.etl_file%ROWTYPE;
    previous control.etl_file%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'));
    SELECT f.*, b.pipeline_name, b.source_name INTO STRICT current_attempt
    FROM control.etl_file f JOIN control.etl_batch b USING (batch_id)
    WHERE f.batch_id = p_batch_id AND f.file_status = 'STAGED' AND b.status = 'STAGED';

    SELECT a.* INTO STRICT approval
    FROM control.etl_file a JOIN control.etl_batch b USING (batch_id)
    WHERE a.etl_file_id = p_approval_file_id
      AND b.pipeline_name = current_attempt.pipeline_name AND b.source_name = current_attempt.source_name
      AND a.source_file = current_attempt.source_file AND a.source_year_month = current_attempt.source_year_month
      AND a.file_status = 'APPROVED_SOURCE_VERSION' AND a.approval_status = 'APPROVED'
      AND a.approved_by IS NOT NULL AND a.approved_at IS NOT NULL
      AND a.file_version_id = current_attempt.file_version_id
      AND a.source_file_sha256 = current_attempt.source_file_sha256;

    SELECT f.* INTO previous
    FROM control.etl_file f JOIN control.etl_batch b USING (batch_id)
    WHERE b.pipeline_name = current_attempt.pipeline_name AND b.source_name = current_attempt.source_name
      AND f.source_file = current_attempt.source_file AND f.source_year_month = current_attempt.source_year_month
      AND f.file_status = 'STAGED' AND b.status = 'SUCCESS'
    ORDER BY b.ended_at DESC NULLS LAST, b.batch_id DESC LIMIT 1;
    IF previous.file_version_id IS NULL
       OR previous.source_file_sha256 IS DISTINCT FROM approval.previous_sha256
       OR previous.file_version_id = current_attempt.file_version_id THEN
        RAISE EXCEPTION 'Approval must target the last successfully published version';
    END IF;

    IF EXISTS (
        SELECT 1 FROM control.etl_replacement r JOIN control.etl_batch b USING (batch_id)
        WHERE b.status <> 'SUCCESS'
    ) THEN
        RAISE EXCEPTION 'An unpublished replacement already exists';
    END IF;
    INSERT INTO control.etl_replacement(batch_id, old_version_id, new_version_id, approval_file_id)
    VALUES (p_batch_id, previous.file_version_id, current_attempt.file_version_id, p_approval_file_id);
END
$$;

CREATE OR REPLACE FUNCTION control.guard_replacement_batch_start()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.pipeline_name = 'pl_taxi_master' THEN
        PERFORM pg_advisory_xact_lock(hashtext('yellow_taxi_staging_writer'));
        IF EXISTS (
            SELECT 1 FROM control.etl_replacement r JOIN control.etl_batch b USING (batch_id)
            WHERE b.status <> 'SUCCESS'
        ) THEN
            RAISE EXCEPTION 'An unpublished replacement must be recovered before starting another batch';
        END IF;
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE TRIGGER guard_replacement_batch_start
BEFORE INSERT ON control.etl_batch FOR EACH ROW
EXECUTE FUNCTION control.guard_replacement_batch_start();

COMMIT;
