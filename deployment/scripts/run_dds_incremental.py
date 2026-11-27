"""Scheduler entry point: run the latest validated catalog file through DDS.

Configure DDS_EXPECTED_PEAK_BYTES and call this script from one scheduler.
The controller owns the single-writer lock, idempotent skip and cleanup.
Reporting has its own job/checkpoint; this entry point never builds OLAP.
"""
from __future__ import annotations

import csv
import os
from pathlib import Path
import sys

import run_24_month_backfill as runner


def trigger() -> int:
    forecast = os.environ.get('DDS_EXPECTED_PEAK_BYTES')
    if not forecast or not forecast.isdecimal() or int(forecast) <= 0:
        raise RuntimeError('DDS_EXPECTED_PEAK_BYTES must be a positive disk forecast')
    catalog = Path(os.environ.get('DDS_CATALOG_PATH', runner.CATALOG)).resolve()
    if not catalog.is_file():
        raise RuntimeError('Validated DDS catalog is missing')
    with catalog.open(newline='', encoding='utf-8') as stream:
        rows = [row for row in csv.DictReader(stream)
                if row.get('validation_status') == 'VALIDATED']
    if not rows:
        raise RuntimeError('No validated file in DDS catalog')
    month = max(row['source_year_month'] for row in rows)
    runner.CATALOG = catalog
    argv = ['run_dds_incremental', '--run-type', 'INCREMENTAL',
            '--from-month', month, '--to-month', month,
            '--enable-cleanup', '--expected-peak-bytes', forecast]
    print('DDS scheduler trigger: {} ({})'.format(month, catalog), flush=True)
    previous_argv = sys.argv
    try:
        sys.argv = argv
        return runner.main()
    finally:
        sys.argv = previous_argv


if __name__ == '__main__':
    raise SystemExit(trigger())
