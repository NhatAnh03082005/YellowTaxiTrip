"""Exercise archive rollback and scope on the already approved QA replacement."""
import os
import subprocess
from unittest.mock import patch

from run_storage_fixture import configure_fixture, runner


def fact_snapshot():
    return runner.execute("""
        SELECT file_version_id, count(*) AS n,
               md5(string_agg(trim(source_record_key)||':'||trip_key, ',' ORDER BY source_record_key)) AS identity
        FROM dds.fact_taxi_trip GROUP BY file_version_id ORDER BY file_version_id
    """, fetch=True)


def main():
    os.environ['NYC_STORAGE_QA_REPLACEMENT'] = '1'
    configure_fixture()
    before = {row['file_version_id']: dict(row) for row in fact_snapshot()}
    if set(before) != {1, 2, 4}:
        raise RuntimeError('Expected the original QA versions and approved replacement from Task 4')
    with runner.runner_lock():
        # The normal runner continues blocking replacement until OLAP recovery has been validated.
        with patch.object(runner, 'require_safe_file_version'):
            staged = runner.run_ingestion('2024-05', 4, 'storage_fixture_qa', 'Test guarded archive scope')
        batch = staged['batch_id']
        args = ['run', '--select', 'fact_taxi_trip']
        variables = {'apply_approved_fact_replacement': True}
        try:
            runner.run_dbt(args, batch, {
                **variables, 'fixture_mode': True, 'fact_lifecycle_fail_after_archive': True,
            })
        except subprocess.CalledProcessError:
            if {row['file_version_id']: dict(row) for row in fact_snapshot()} != before:
                raise RuntimeError('Failed archive changed Fact')
            if runner.execute("""
                SELECT count(*) AS n FROM dq.fact_taxi_trip_history
                WHERE change_type='REPLACE_VERSION' AND batch_id=%(batch)s
            """, {'batch': batch}, fetch=True)[0]['n']:
                raise RuntimeError('Failed archive event was committed')
        else:
            raise RuntimeError('Expected archive fault')
        runner.run_dbt(args, batch, variables)
        after = {row['file_version_id']: dict(row) for row in fact_snapshot()}
        if after != {k: before[k] for k in (2, 4)}:
            raise RuntimeError('Replacement modified another file version or retained old Fact')
        events = runner.execute("""
            SELECT count(*) AS n, count(distinct source_record_key) AS keys
            FROM dq.fact_taxi_trip_history
            WHERE change_type='REPLACE_VERSION' AND batch_id=%(batch)s
              AND (prior_fact->>'file_version_id')::bigint=1
        """, {'batch': batch}, fetch=True)[0]
        if events['n'] != before[1]['n'] or events['keys'] != events['n']:
            raise RuntimeError('Incomplete archive lineage')
        runner.run_dbt(['run', '--select', 'tag:fingerprint_candidate'], batch, {
            'enable_fingerprint_history_candidate': True, 'fingerprint_parity_file_version': 1,
        })
        runner.run_dbt(['test', '--select', 'fingerprint_history_candidate_parity', 'fact_lifecycle_history'], batch, {
            'enable_fingerprint_history_candidate': True, 'fingerprint_parity_file_version': 1,
        })
        runner.execute("""
            SELECT control.fail_batch(%(batch)s,'QA_FACT_REPLACEMENT_ONLY',
                'Fact archive checks passed. OLAP and batch publication not run.');
        """, {'batch': batch})
        print(f'QA batch {batch}: archived {events["n"]} Fact rows, retained identities for versions 2 and 4')


if __name__ == '__main__':
    main()
