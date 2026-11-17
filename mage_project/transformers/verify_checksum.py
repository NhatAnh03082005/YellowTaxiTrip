from pathlib import Path

import pandas as pd

from utils.database import execute, fetch_one
from utils.parquet_utils import calculate_file_sha256, inspect_parquet
from utils.paths import resolve_data_path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def verify_checksum(discovery_context, *args, **kwargs):
    batch_id = int(discovery_context['batch_id'])
    registry = pd.read_csv(Path(kwargs.get(
        'schema_registry_path',
        '/home/src/data/catalog/yellow_taxi_schema_registry.csv',
    )), dtype='string')
    versions = dict(zip(registry['schema_fingerprint'], registry['schema_version']))
    execute("SELECT control.start_step(%(batch_id)s, 'FILE_VALIDATION');", {'batch_id': batch_id})
    validated = []
    try:
        for context in discovery_context['files']:
            etl_file_id = int(context['etl_file_id'])
            path = resolve_data_path(context['local_path'])
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
            validated.append({
                **context, 'resolved_path': str(path), 'source_file_sha256': sha256,
                'schema_fingerprint': metadata['schema_fingerprint'],
                'schema_version': schema_version, 'parquet_row_count': metadata['row_count'],
                'row_group_count': metadata['row_group_count'], 'action': action,
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
