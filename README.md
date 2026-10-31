# NYC Yellow Taxi source survey and data quality

Reproducible survey of official NYC TLC Yellow Taxi files, representative full-file DQ checks, and Taxi Zone reference/spatial profiling. Raw inputs are never overwritten.

## Environment

PowerShell from the project parent:

```powershell
uv venv .venv --python 3.12
uv pip install --python .venv\Scripts\python.exe -r nyc_taxi_survey\requirements.txt
```

## Inputs

Place these unchanged in `nyc_taxi_survey/data/reference/`:

- `taxi+_zone_lookup.csv`
- `taxi_zones.zip`
- optional project brief PDF

Official Yellow Taxi Parquet files are cached in `data/raw/`. Every representative download receives size and SHA-256 evidence in `outputs/profiles/representative_files.csv`.

## Run order

```powershell
.venv\Scripts\python.exe nyc_taxi_survey\scripts\crawl_tlc_manifest.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\download_representative.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_parquet.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_taxi_zone_csv.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_shapefile.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\run_data_quality_checks.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\profile_legacy_spatial.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\recompute_duplicate_counts.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\enrich_temporal_fee_checks.py
.venv\Scripts\python.exe nyc_taxi_survey\scripts\generate_deliverables.py
```

Scripts write timestamped logs under `logs/`. Re-running downloads reuses any complete non-zero local file. A `.part` file is temporary; rerun after a network failure. To change representative years/months, edit `YEARS` and the January preference in `download_representative.py`; selection reason is recorded. If a URL is absent/broken, the manifest preserves `missing_from_page`/HTTP status and the selector chooses the earliest available month for that year.

## Outputs

- `outputs/profiles/01...08...csv`: manifest, Parquet/schema evolution, lookup and shapefile evidence.
- `outputs/anomalies/09...11...`: rule registry, anomaly summary and bounded samples.
- `outputs/mappings/12_source_to_target_mapping.csv`: preliminary STM.
- `outputs/reports/NYC_Yellow_Taxi_Source_Survey_and_Data_Quality_Report.md`: final report.

The P99 rules are statistical review flags, not automatic rejection rules. Historical string-code domains are not judged using the current dictionary without version evidence.
