import pandas as pd

from utils.database import require_tables
from utils.stage_loader import upsert_dataframe

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def write_technical_quarantine(context, *args, **kwargs):
    require_tables(['dq.technical_quarantine'])
    written = 0
    for chunk in context['chunks']:
        rejected = pd.read_parquet(chunk['reject_path'])
        written += upsert_dataframe(rejected, 'dq', 'technical_quarantine')
    return {**context, 'technical_quarantine_write_attempts': written}
