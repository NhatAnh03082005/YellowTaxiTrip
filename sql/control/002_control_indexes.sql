\set ON_ERROR_STOP on

CREATE UNIQUE INDEX IF NOT EXISTS uq_etl_batch_one_active_source
    ON control.etl_batch (pipeline_name, source_name)
    WHERE status IN (
        'CREATED', 'RUNNING', 'STAGED', 'DBT_RUNNING',
        'RECONCILING', 'STALE'
    );

CREATE INDEX IF NOT EXISTS ix_etl_batch_pipeline_source_status
    ON control.etl_batch (pipeline_name, source_name, status);

CREATE INDEX IF NOT EXISTS ix_etl_batch_parent
    ON control.etl_batch (parent_batch_id)
    WHERE parent_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_etl_batch_heartbeat
    ON control.etl_batch (heartbeat_at)
    WHERE status IN (
        'CREATED', 'RUNNING', 'STAGED', 'DBT_RUNNING',
        'RECONCILING', 'STALE'
    );

CREATE INDEX IF NOT EXISTS ix_etl_batch_started_at
    ON control.etl_batch (started_at DESC);

CREATE INDEX IF NOT EXISTS ix_etl_file_batch
    ON control.etl_file (batch_id);

CREATE INDEX IF NOT EXISTS ix_etl_file_identity
    ON control.etl_file (source_file, source_file_sha256)
    WHERE source_file_sha256 IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_etl_file_status_month
    ON control.etl_file (file_status, source_year_month);

CREATE INDEX IF NOT EXISTS ix_etl_file_last_modified
    ON control.etl_file (source_last_modified)
    WHERE source_last_modified IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_etl_file_approval
    ON control.etl_file (approval_status, source_file, source_file_sha256)
    WHERE approval_status IN ('PENDING', 'APPROVED');

CREATE INDEX IF NOT EXISTS ix_etl_step_run_batch_status
    ON control.etl_step_run (batch_id, status);

CREATE INDEX IF NOT EXISTS ix_etl_step_run_name_status
    ON control.etl_step_run (step_name, status);

CREATE INDEX IF NOT EXISTS ix_etl_log_batch_time
    ON control.etl_log (batch_id, logged_at)
    WHERE batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_etl_log_step_time
    ON control.etl_log (step_run_id, logged_at)
    WHERE step_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_etl_log_level_time
    ON control.etl_log (log_level, logged_at);
