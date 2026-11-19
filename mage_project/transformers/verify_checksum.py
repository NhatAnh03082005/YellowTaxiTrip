from pathlib import Path

import pandas as pd

from utils.database import execute, fetch_one
if 'transformer' not in globals():
    from utils.decorators import transformer
from utils.parquet_utils import calculate_file_sha256, inspect_parquet
from utils.paths import resolve_data_path

def _resume_successful_validation(discovery_context, data_root=None):
    """Rebuild block output when Mage reruns an already-successful step."""
    validated = []
    for context in discovery_context['files']:
        file_row = fetch_one(
            """
            SELECT file_status, content_length, source_file_sha256,
                   schema_fingerprint, schema_version, file_version_id
            FROM control.etl_file
            WHERE etl_file_id = %(etl_file_id)s
              AND batch_id = %(batch_id)s;
            """,
            {
                'etl_file_id': int(context['etl_file_id']),
                'batch_id': int(discovery_context['batch_id']),
            },
        )
        if not file_row:
            raise RuntimeError(
                'Validated control file is missing: {}'.format(
                    context['etl_file_id']
                )
            )
        action_by_status = {
            'VALIDATED': 'PROCESS',
            'APPROVED_SOURCE_VERSION': 'PROCESS',
            'SKIPPED_ALREADY_PROCESSED': 'SKIP_ALREADY_PROCESSED',
        }
        action = action_by_status.get(file_row['file_status'])
        if action is None:
            raise RuntimeError(
                'Cannot resume FILE_VALIDATION from file status {} for {}'.format(
                    file_row['file_status'], context['file_name']
                )
            )
        path = resolve_data_path(context['local_path'], data_root=data_root)
        metadata = inspect_parquet(path)
        validated.append({
            **context,
            'resolved_path': str(path),
            'source_file_sha256': file_row['source_file_sha256'].strip(),
            'schema_fingerprint': file_row['schema_fingerprint'].strip(),
            'schema_version': file_row['schema_version'],
            'file_version_id': int(file_row['file_version_id']),
            'content_length': int(file_row['content_length']),
            'parquet_row_count': metadata['row_count'],
            'row_group_count': metadata['row_group_count'],
            'action': action,
        })
    return {**discovery_context, 'files': validated}


@transformer
def verify_checksum(discovery_context, *args, **kwargs):
    batch_id = int(discovery_context['batch_id'])
    registry = pd.read_csv(Path(kwargs.get(
        'schema_registry_path',
        '/home/src/data/catalog/yellow_taxi_schema_registry.csv',
    )), dtype='string')
    versions = dict(zip(registry['schema_fingerprint'], registry['schema_version']))

    step = fetch_one(
        """
        SELECT status
        FROM control.etl_step_run
        WHERE batch_id = %(batch_id)s
          AND step_name = 'FILE_VALIDATION';
        """,
        {'batch_id': batch_id},
    )
    if not step:
        raise RuntimeError('FILE_VALIDATION step is missing for batch {}'.format(batch_id))
    if step['status'] == 'SUCCESS':
        return _resume_successful_validation(
            discovery_context, data_root=kwargs.get('data_root')
        )
    if step['status'] != 'PENDING':
        raise RuntimeError(
            'FILE_VALIDATION for batch {} is {}; start a new pipeline execution'.format(
                batch_id, step['status']
            )
        )

    execute("SELECT control.start_step(%(batch_id)s, 'FILE_VALIDATION');", {'batch_id': batch_id})
    validated = []
    try:
        for context in discovery_context['files']:
            etl_file_id = int(context['etl_file_id'])
            path = resolve_data_path(
                context['local_path'], data_root=kwargs.get('data_root')
            )
            execute('SELECT control.mark_file_downloading(%(id)s);', {'id': etl_file_id})
            if not path.is_file():
                raise FileNotFoundError('Parquet not found: {}'.format(path))
            sha256 = calculate_file_sha256(path)
            metadata = inspect_parquet(path)
            if sha256 != context['sha256'].lower():
                raise ValueError('SHA-256 mismatch for {}'.format(context['file_name']))
            schema_version = versions.get(metadata['schema_fingerprint'])
            if schema_version is None:
                raise ValueError('Unregistered schema fingerprint: {}'.format(
                    metadata['schema_fingerprint']
                ))
            action = fetch_one(
                """
                SELECT control.validate_file(
                    %(id)s, %(size)s, %(sha)s, %(fingerprint)s, %(version)s
                ) AS action;
                """,
                {'id': etl_file_id, 'size': path.stat().st_size, 'sha': sha256,
                 'fingerprint': metadata['schema_fingerprint'], 'version': schema_version},
            )['action']
            if action == 'STOP_SOURCE_VERSION_CHANGED':
                raise RuntimeError('Source version changed: {}'.format(context['file_name']))
            version_row = fetch_one(
                """
                SELECT file_version_id
                FROM control.etl_file
                WHERE etl_file_id = %(id)s;
                """,
                {'id': etl_file_id},
            )
            if not version_row or version_row['file_version_id'] is None:
                raise RuntimeError(
                    'Stable file version was not assigned for {}'.format(
                        context['file_name']
                    )
                )
            validated.append({
                **context, 'resolved_path': str(path), 'source_file_sha256': sha256,
                'schema_fingerprint': metadata['schema_fingerprint'],
                'schema_version': schema_version, 'parquet_row_count': metadata['row_count'],
                'row_group_count': metadata['row_group_count'], 'action': action,
                'file_version_id': int(version_row['file_version_id']),
            })
        execute(
            """
            SELECT control.finish_step(
                %(batch_id)s, 'FILE_VALIDATION', 'SUCCESS', 0,
                jsonb_build_object('files_validated', %(count)s), NULL
            );
            """,
            {'batch_id': batch_id, 'count': len(validated)},
        )
    except Exception as exception:
        execute(
            """
            SELECT control.finish_step(%(batch_id)s, 'FILE_VALIDATION', 'FAILED', 1,
                                       '{}'::jsonb, %(message)s);
            SELECT control.fail_batch(%(batch_id)s, 'FILE_VALIDATION_FAILED', %(message)s);
            """,
            {'batch_id': batch_id, 'message': str(exception)[:2000]},
        )
        raise
    return {**discovery_context, 'files': validated}
