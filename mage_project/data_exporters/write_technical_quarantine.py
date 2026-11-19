import pandas as pd

from utils.database import require_tables
from utils.stage_loader import upsert_dataframe
if 'data_exporter' not in globals():
    from utils.decorators import data_exporter

@data_exporter
def write_technical_quarantine(ingestion_context, *args, **kwargs):
    require_tables(['dq.technical_quarantine'])
    written = 0
    for chunk in ingestion_context['chunks']:
        rejected = pd.read_parquet(chunk['reject_path'])
        written += upsert_dataframe(rejected, 'dq', 'technical_quarantine')
    return {
        **ingestion_context,
        'technical_quarantine_write_attempts': written,
    }
