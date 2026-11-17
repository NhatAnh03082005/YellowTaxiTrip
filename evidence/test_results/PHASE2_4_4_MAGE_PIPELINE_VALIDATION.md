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

The exporters therefore remain intentionally unexecuted. They use `require_tables` and cannot silently create or replace the 4.5 physical schema. Full database integration and 24-file backfill evidence must be collected after task 4.5 DDL is deployed.
