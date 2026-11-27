from utils.profiling import profiled
if 'transformer' not in globals():
    from utils.decorators import transformer


@transformer
@profiled
def standardize_and_validate(ingestion_context, *args, **kwargs):
    if ingestion_context.get('prepare_mode')=='streaming':
        return ingestion_context
    rows_raw = sum(item['row_count'] for item in ingestion_context['chunks'])
    rows_canonical = sum(item['canonical_rows'] for item in ingestion_context['chunks'])
    rows_rejected = sum(
        item['technical_reject_rows'] for item in ingestion_context['chunks']
    )
    if rows_raw != rows_canonical + rows_rejected:
        raise ValueError('rows_raw != rows_canonical + rows_technical_quarantined')
    return {
        **ingestion_context,
        'metrics': {
            'rows_raw': rows_raw, 'rows_canonical': rows_canonical,
            'rows_technical_quarantined': rows_rejected,
        },
    }
