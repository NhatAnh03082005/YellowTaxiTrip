from pathlib import Path

import pandas as pd

from utils.database import execute, fetch_one
if 'transformer' not in globals():
    from utils.decorators import transformer


@transformer
def discover_source_files(batch_context, *args, **kwargs):
    batch_id = int(batch_context['batch_id'])
    catalog_path = Path(kwargs.get(
        'catalog_path', '/home/src/data/catalog/yellow_taxi_2024_2025.csv'
    ))
    catalog = pd.read_csv(catalog_path, dtype='string')
    month_from = kwargs.get('source_year_month_from', '2024-01')
    month_to = kwargs.get('source_year_month_to', '2025-12')
    catalog = catalog[
        (catalog['source_year_month'] >= month_from)
        & (catalog['source_year_month'] <= month_to)
    ].copy()
    if catalog.empty:
        raise ValueError('Catalog scope is empty: {}..{}'.format(month_from, month_to))
    expected_count = len(catalog)
    execute("SELECT control.start_step(%(batch_id)s, 'DISCOVERY');", {'batch_id': batch_id})
    selected_files = []
    try:
        for record in catalog.to_dict(orient='records'):
            selected = fetch_one(
                """
                SELECT control.should_discover_file(
                    %(batch_id)s, %(source_file)s,
                    %(source_last_modified)s::timestamptz, %(sha256)s
                ) AS selected;
                """,
                {
                    'batch_id': batch_id, 'source_file': record['file_name'],
                    'source_last_modified': record['last_modified'],
                    'sha256': record['sha256'],
                },
            )
            if not selected['selected']:
                continue
            file_row = fetch_one(
                """
                SELECT control.discover_file(
                    %(batch_id)s, %(source_file)s, %(source_year_month)s,
                    %(source_url)s, %(source_last_modified)s::timestamptz
                ) AS etl_file_id;
                """,
                {
                    'batch_id': batch_id, 'source_file': record['file_name'],
                    'source_year_month': record['source_year_month'],
                    'source_url': record['source_url'],
                    'source_last_modified': record['last_modified'],
                },
            )
            selected_files.append({
                **record, 'batch_id': batch_id,
                'etl_file_id': int(file_row['etl_file_id']),
            })
        execute(
            """
            SELECT control.update_discovery_result(
                %(batch_id)s, 'SUCCESS', %(expected)s, %(checked)s, %(selected)s
            );
            SELECT control.finish_step(
                %(batch_id)s, 'DISCOVERY', 'SUCCESS', 0,
                jsonb_build_object('scope_from', %(scope_from)s,
                                   'scope_to', %(scope_to)s,
                                   'selected', %(selected)s), NULL
            );
            """,
            {'batch_id': batch_id, 'expected': expected_count, 'checked': expected_count,
             'selected': len(selected_files), 'scope_from': month_from, 'scope_to': month_to},
        )
    except Exception as exception:
        execute(
            """
            SELECT control.finish_step(%(batch_id)s, 'DISCOVERY', 'FAILED', 1,
                                       '{}'::jsonb, %(message)s);
            SELECT control.fail_batch(%(batch_id)s, 'DISCOVERY_FAILED', %(message)s);
            """,
            {'batch_id': batch_id, 'message': str(exception)[:2000]},
        )
        raise
    return {
        **batch_context, 'catalog_expected_count': expected_count,
        'catalog_checked_count': expected_count,
        'files_selected': len(selected_files), 'files': selected_files,
    }
