"""Exercise actual dbt transactions and runner resume in the isolated 131-row fixture."""
import os
import subprocess
import sys
from unittest.mock import patch

from run_fact_replacement_qa import fact_snapshot
from run_storage_fixture import configure_fixture, runner

DATABASE = 'nyc_taxi_qa_replacement_recovery'


def one(sql, **params):
    return dict(runner.execute(sql, params, fetch=True)[0])


def watermark():
    return one("""SELECT watermark_version, last_successful_batch_id, last_successful_extraction_time
                  FROM control.etl_watermark WHERE pipeline_name='pl_taxi_master'
                    AND source_name='yellow_taxi_tripdata'""")


def failure_state(batch, baseline):
    assert watermark() == baseline, 'Watermark changed on failure'
    assert one("""SELECT status FROM control.etl_batch WHERE batch_id=%(batch)s""", batch=batch)['status'] == 'FAILED'
    assert one("""SELECT count(*) AS n FROM control.file_reconciliation_history
                  WHERE successful_batch_id=%(batch)s""", batch=batch)['n'] == 0
    assert one("""SELECT count(*) AS n FROM stage.yellow_taxi_raw
                  WHERE batch_id=%(batch)s""", batch=batch)['n'] == 131
    try:
        runner.require_no_pending_replacement()
    except RuntimeError as exc:
        assert f'Replacement pending for batch {batch}' in str(exc)
    else:
        raise AssertionError('Unpublished replacement must block new work')
    runner.require_no_pending_replacement(batch)


def archive_count(batch):
    return one("""SELECT count(*) n FROM dq.fact_taxi_trip_history
                  WHERE batch_id=%(batch)s AND change_type='REPLACE_VERSION'
                    AND (prior_fact->>'file_version_id')::bigint=4""", batch=batch)['n']


def main():
    os.environ['NYC_STORAGE_QA_REPLACEMENT'] = '1'
    configure_fixture(DATABASE, replacement_rows=130)
    run_dbt = runner.run_dbt
    execute_cursor = runner.execute_cursor
    before = {row['file_version_id']: dict(row) for row in fact_snapshot()}
    continuing = '--continue-qa' in sys.argv
    if not continuing:
        assert set(before) == {2, 4}
    baseline = watermark()

    with runner.runner_lock():
        if continuing:
            batch = one("""SELECT r.batch_id FROM control.etl_replacement r
                         JOIN control.etl_batch b USING(batch_id)
                         WHERE b.status='FAILED' AND r.old_version_id=4""")['batch_id']
            before[4] = {'n': archive_count(batch)}
            assert before[4]['n'] == 127
            failure_state(batch, baseline)
        else:
            batch = prepare_and_fail_fact(before, baseline, run_dbt)

        def fail_between_olap(args, batch_id, variables=None):
            if args == ['run', '--select', 'tag:olap']:
                run_dbt(['run', '--select', 'olap_daily_kpi'], batch_id, variables)
                raise RuntimeError('QA interruption after first OLAP model')
            run_dbt(args, batch_id, variables)

        with patch.object(runner, 'run_dbt', side_effect=fail_between_olap):
            try:
                runner.run_downstream(batch)
            except RuntimeError as exc:
                assert str(exc) == 'QA interruption after first OLAP model', str(exc)
            else:
                raise AssertionError('Expected partial OLAP failure')
        failure_state(batch, baseline)
        retained = {row['file_version_id']: dict(row) for row in fact_snapshot()}
        assert 4 not in retained and retained[2] == before[2]
        assert archive_count(batch) == before[4]['n']
        print('PASS partial OLAP failure kept publication pending', flush=True)

        def fail_after_commit_function(cursor, sql, params=None, fetch=False):
            result = execute_cursor(cursor, sql, params, fetch)
            if 'control.commit_batch_success' in sql:
                cursor.execute('SELECT 1/0')
            return result

        with patch.object(runner, 'execute_cursor', side_effect=fail_after_commit_function):
            try:
                runner.run_downstream(batch)
            except runner.psycopg2.errors.DivisionByZero:
                pass
            else:
                raise AssertionError('Expected publication transaction rollback')
        failure_state(batch, baseline)
        assert archive_count(batch) == before[4]['n']
        assert {row['file_version_id']: dict(row) for row in fact_snapshot()} == retained
        print('PASS watermark and reconciliation rolled back together', flush=True)

        runner.run_downstream(batch)
        after = watermark()
        assert after['last_successful_batch_id'] == batch
        assert after['watermark_version'] == baseline['watermark_version'] + 1
        assert one("""SELECT count(*) n FROM control.file_reconciliation_history
                      WHERE successful_batch_id=%(batch)s""", batch=batch)['n'] == 1
        assert runner.run_downstream(batch) == {'batch_id': batch, 'already_committed': True}
        assert watermark() == after
        assert archive_count(batch) == before[4]['n']
        assert {row['file_version_id']: dict(row) for row in fact_snapshot()} == retained
        runner.require_no_pending_replacement()
        print(f'PASS retry and idempotent acknowledgement, QA batch {batch}', flush=True)


def prepare_and_fail_fact(before, baseline, run_dbt):
    try:
        runner.run_ingestion('2024-05')
    except RuntimeError as exc:
        assert 'Source version changed:' in str(exc), str(exc)
    else:
        raise AssertionError('Missing unapproved version gate')
    review = one("""SELECT etl_file_id FROM control.etl_file
                     WHERE file_status='SOURCE_VERSION_CHANGED'
                     ORDER BY etl_file_id DESC LIMIT 1""")['etl_file_id']
    runner.execute("SELECT control.approve_source_version(%(id)s,'qa_fixture','recovery fixture only')",
                   {'id': review})
    # Keep the production ingestion gate in place. Only this isolated harness binds the staged pair.
    with patch.object(runner, 'require_safe_file_version'):
        batch = runner.run_ingestion('2024-05')['batch_id']
    runner.execute('SELECT control.bind_approved_replacement(%(batch)s,%(review)s)',
                   {'batch': batch, 'review': review})

    def fail_fact(args, batch_id, variables=None):
        if args == ['run', '--exclude', 'tag:olap']:
            run_dbt(args, batch_id, {**variables, 'fixture_mode': True,
                                    'fact_lifecycle_fail_after_archive': True})
        else:
            run_dbt(args, batch_id, variables)

    with patch.object(runner, 'run_dbt', side_effect=fail_fact):
        try:
            runner.run_downstream(batch)
        except subprocess.CalledProcessError:
            pass
        else:
            raise AssertionError('Expected Fact transaction failure')
    failure_state(batch, baseline)
    assert {row['file_version_id']: dict(row) for row in fact_snapshot()} == before
    assert archive_count(batch) == 0
    print('PASS Fact rollback with preserved Staging', flush=True)
    return batch


if __name__ == '__main__':
    main()
