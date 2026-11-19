if 'transformer' not in globals():
    from utils.decorators import transformer


@transformer
def filter_incremental(validation_context, *args, **kwargs):
    process_actions = {'PROCESS', 'REPROCESS_IDEMPOTENTLY'}
    process_files = [
        item for item in validation_context['files']
        if item['action'] in process_actions
    ]
    skipped = [
        item for item in validation_context['files']
        if item['action'] == 'SKIP_ALREADY_PROCESSED'
    ]
    return {
        **validation_context, 'files': process_files,
        'process_file_count': len(process_files), 'skipped_file_count': len(skipped),
    }
