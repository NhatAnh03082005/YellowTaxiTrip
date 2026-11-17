\set ON_ERROR_STOP on

DO $$
DECLARE
    v_ok boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM control.etl_file f
        JOIN control.etl_batch b ON b.batch_id = f.batch_id
        WHERE b.pipeline_name = 'qa_43_t18_file'
          AND f.approval_status = 'APPROVED'
          AND f.approved_by = 'project_lead'
          AND f.approved_at IS NOT NULL
          AND NULLIF(btrim(f.approval_reason), '') IS NOT NULL
    ) INTO v_ok;
    IF NOT v_ok THEN
        RAISE EXCEPTION 'Approval audit fields are missing';
    END IF;
    RAISE NOTICE 'PASS T18D: approval actor, timestamp and reason persisted';
END;
$$;
