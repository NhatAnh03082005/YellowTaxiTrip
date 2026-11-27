from utils.profiling import profiled, phase
import pandas as pd

from utils.database import require_tables
from importlib import reload
import utils.stage_loader as stage_loader

if 'data_exporter' not in globals():
    from utils.decorators import data_exporter


@data_exporter
@profiled
def write_technical_quarantine(ingestion_context, *args, **kwargs):
    mode = kwargs.get('stage_load_mode', ingestion_context.get('stage_load_mode','upsert'))
    if mode not in ('upsert','direct'):
        raise ValueError('Unknown stage_load_mode: '+str(mode))
    if ingestion_context.get('quarantine_deferred') and mode!='direct':
        raise ValueError('Deferred quarantine cannot switch to upsert implicitly')
    if mode=='direct' and ingestion_context['files']:
        # Rejects are copied atomically alongside Raw and Canonical by load_stage.
        return {**ingestion_context,'stage_load_mode':'direct','quarantine_deferred':True}
    require_tables(['dq.technical_quarantine'])
    reload(stage_loader)
    written = 0
    counts = {}
    with stage_loader.StageWriter() as writer:
        for chunk in ingestion_context['chunks']:
            if int(chunk['technical_reject_rows'])==0:
                continue
            with phase('read_reject_parquet'):
                rejected = pd.read_parquet(chunk['reject_path'])
            if len(rejected) != int(chunk['technical_reject_rows']):
                raise ValueError('Technical reject part count mismatch')
            written += stage_loader.upsert_dataframe(rejected, 'dq', 'technical_quarantine', metrics=counts, writer=writer)
    return {
        **ingestion_context,
        'technical_quarantine_write_attempts': written,
        'technical_quarantine_write_counts': counts,
    }
