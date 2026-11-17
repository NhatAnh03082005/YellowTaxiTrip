\set ON_ERROR_STOP on

DO $$
DECLARE
    v_tables integer;
    v_functions integer;
    v_named_indexes integer;
    v_comments integer;
BEGIN
    SELECT count(*) INTO v_tables
    FROM information_schema.tables WHERE table_schema = 'control';
    SELECT count(*) INTO v_functions
    FROM information_schema.routines WHERE routine_schema = 'control';
    SELECT count(*) INTO v_named_indexes
    FROM pg_indexes
    WHERE schemaname = 'control'
      AND indexname LIKE 'ix\_%' ESCAPE '\';
    SELECT count(*) INTO v_comments
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'control' AND c.relkind = 'r'
      AND obj_description(c.oid, 'pg_class') IS NOT NULL;

    IF v_tables <> 5 OR v_functions <> 18
       OR v_named_indexes < 14 OR v_comments <> 5 THEN
        RAISE EXCEPTION 'Schema mismatch: tables %, functions %, named indexes %, comments %',
            v_tables, v_functions, v_named_indexes, v_comments;
    END IF;
    RAISE NOTICE 'PASS T15: 5 tables, 18 functions, indexes and table comments';
END;
$$;
