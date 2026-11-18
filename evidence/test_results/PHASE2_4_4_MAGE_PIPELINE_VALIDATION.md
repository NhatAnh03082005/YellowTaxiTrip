# Phase 2 task 4.4 — Mage pipeline validation

**Date:** 2026-11-17 (Asia/Bangkok)  
**Environment:** Docker Mage 0.9.76, project `nyc_taxi_bi`

## Automated tests

Command:

```powershell
docker compose exec -T mage bash -lc "cd /home/src/mage_project && python -m unittest discover -s nyc_taxi_bi/tests -t . -p 'test_task_4_4*.py' -v"
```

Result:

```text
Ran 11 tests in 0.289s
OK
```

Validated behavior:

- Portable catalog path resolution and traversal protection.
- Streaming file SHA-256.
- Ordered Parquet schema fingerprint.
- Stable source record identity.
- Technical canonical/reject split and exact row reconciliation.
- Business anomalies remain outside technical quarantine.
- JSON work-file serialization supports empty error dictionaries.
- All 24 catalog files exist and are marked VALIDATED.
- Representative 2024/2025 files resolve to schema v001/v002.
- Taxi Zone contract is 265 lookup rows, 263 geometries, EPSG:2263 and only IDs 264/265 without geometry.

## Mage metadata validation

Mage loaded both pipelines after restart:

```text
pl_reference_taxi_zone: 5 blocks
pl_yellow_taxi_ingestion: 10 blocks
```

No Mage startup error was observed after the pipeline metadata was added.

## Physical Staging gate

`to_regclass` returned null for all five task 4.5 targets:

```text
stage.yellow_taxi_raw
stage.yellow_taxi_canonical
stage.taxi_zone_raw
stage.taxi_zone_geometry
dq.technical_quarantine
```

At this initial checkpoint the exporters remained intentionally unexecuted. They
use `require_tables` and cannot silently create or replace the 4.5 physical
schema. The later reference integration result is recorded below; Yellow Taxi
integration and full backfill evidence remain separate gates.

## Reference pipeline integration update — 2026-11-18

The TLC lookup uses literal text `N/A` for valid source members 264/265.
`load_zone_lookup` now sets `keep_default_na=False` so pandas does not coerce
those values to null. A regression test covers both members.

Reference loading then completed successfully:

```text
stage.taxi_zone_raw:       265 rows
stage.taxi_zone_geometry:  263 rows
geometry SRID:             2263
lookup IDs without polygon: 264, 265
```

The three reference-pipeline checklist items in task 4.4 are therefore complete.

Mage 0.9.76 cannot preview a GeoDataFrame because it calls
`to_json(orient='split')`, while GeoPandas uses a different `to_json` signature.
Both the Shapefile loader and geometry validation block now return normal pandas
DataFrames containing `geometry_wkt`; the loader also carries
`source_crs_epsg`. The validator reconstructs GeoPandas internally, and the
exporter reconstructs an EPSG:2263 GeoDataFrame immediately before `to_postgis`.
A regression test executes the exact Mage preview call at both block boundaries.
The suite now has 13 passing tests, and the reference reload still produces
265 lookup rows and 263 geometries at SRID 2263.

After a Mage service restart exposed that notebook kernels did not include the
project root in `sys.path`, Docker Compose was updated with project-derived
`PYTHONPATH`. The recreated Mage service successfully imports `utils` and all
reference block modules from its normal runtime working directory.

## Pipeline-scoped file-history fix — 2026-11-18

A production backfill initially stopped on `yellow_tripdata_2025-01.parquet`
because a prior QA batch had staged the same filename with synthetic SHA-256
`aaaa...`. The real catalog and local file both match SHA-256
`9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`.

`should_discover_file` and `validate_file` now scope successful, approved and
failed-staged history by `(pipeline_name, source_name)`. The regression test
`test_pipeline_scoped_file_history.sql` confirms that QA history from another
pipeline returns `PROCESS`, not `STOP_SOURCE_VERSION_CHANGED`, for production.
Existing source-version approval and retry evidence checks continue to pass.

## Successful-step resume fix — 2026-11-18

Mage users may manually rerun a block after its control step has already reached
`SUCCESS`. `verify_checksum` now detects a successful `FILE_VALIDATION` step and
rebuilds its output from `control.etl_file` plus Parquet footer metadata instead
of calling `start_step` again. Non-terminal `RUNNING`/`FAILED` states still stop
with an explicit instruction to start a new execution. The live regression on
batch 31 returned all 24 validated files without changing control state.

## Reserved Mage `context` keyword fix — 2026-11-18

Mage passes a runtime keyword named `context` to blocks. Downstream block
functions previously used `context` as their positional upstream-data parameter,
which caused `got multiple values for argument 'context'`. Those parameters are
now named `ingestion_context` in load, canonicalization, validation, quarantine,
stage-load and stage-verification blocks. A regression test invokes the chain
with Mage-style `context=` and passes. The task 4.4 suite now has 14 tests.

## Bounded source-load fix — 2026-11-18

The 24-month backfill was interrupted while pandas converted an entire Parquet
row group back to Arrow for `DataFrame.to_parquet`. The source loader now uses
`ParquetFile.iter_batches` with a default limit of 100,000 rows, appends lineage
as Arrow arrays and writes Snappy Parquet directly. A rerun removes only the
current batch's incomplete `raw` work directory before rebuilding it. The new
regression test verifies bounded part sizes, continuous source row numbers and
unique stable record keys. The full Mage-container suite now has 15 passing
tests.

## Canonical JSON-null and staging-resume fix — 2026-11-18

The first stage-load part exposed a semantic difference between SQL `NULL` and
JSONB `null`: a missing canonical `raw_payload` was serialized as the JSON text
`null`, which correctly failed `ck_yellow_taxi_canonical_raw_payload`.
`stage_loader.serialize_json_value` now preserves Python/pandas missing values as
SQL `NULL` while serializing actual dictionaries and lists. A live insert of
100,000 canonical rows from batch 32 produced 100,000 SQL-null payloads and zero
JSON-null payloads.

Staging now uses PostgreSQL `COPY` into a typed temporary table before its
idempotent `ON CONFLICT` insert. The live 100,000-row canonical smoke part took
about 5.1 seconds. `control.resume_failed_staging` permits only a batch carrying
`STAGING_FAILED` and a failed STAGING step to return to RUNNING/PENDING, records
`STAGING_RESUMED` in the audit log, and avoids recomputing the 910 canonical work
parts. The Mage-container suite now has 16 passing tests.

## Nullable integer COPY fix — 2026-11-18

During the two-month batch, a raw part containing null passenger counts was
read by pandas as `float64`; valid integer values were consequently emitted to
CSV as `1.0`, which PostgreSQL `COPY` cannot parse as `bigint`. The stage loader
now validates every SMALLINT/INTEGER/BIGINT target column and converts it to
pandas nullable `Int64` before CSV serialization. Fractional values fail early
with a targeted error instead of being rounded. Two regression tests cover the
nullable and fractional cases; the Mage-container suite now has 18 passing
tests. A live smoke load of the formerly failing 100,000-row part succeeded for
both raw and canonical tables.
