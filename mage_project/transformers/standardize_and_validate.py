if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def standardize_and_validate(context, *args, **kwargs):
    rows_raw = sum(item['row_count'] for item in context['chunks'])
    rows_canonical = sum(item['canonical_rows'] for item in context['chunks'])
    rows_rejected = sum(item['technical_reject_rows'] for item in context['chunks'])
    if rows_raw != rows_canonical + rows_rejected:
        raise ValueError('rows_raw != rows_canonical + rows_technical_quarantined')
    return {
        **context,
        'metrics': {
            'rows_raw': rows_raw, 'rows_canonical': rows_canonical,
            'rows_technical_quarantined': rows_rejected,
        },
    }
