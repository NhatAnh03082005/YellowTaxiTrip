-- T19A uses two real PostgreSQL sessions. Session A holds the advisory
-- transaction lock while Session B calls start_batch for the same source.
-- The PowerShell runner records that B waits, then fails with
-- "Active or STALE batch already exists" after A commits.

SELECT batch_id, pipeline_name, source_name, status
FROM control.etl_batch
WHERE pipeline_name = 'qa_43_t19_concurrent'
ORDER BY batch_id;
