# Phase 2 task 4.5 — Staging schema and index validation

**Date:** 2026-11-17 (Asia/Bangkok)  
**Scope:** Physical tables, constraints, comments and indexes through implementation step 8.

## Migration

Created and deployed:

```text
stage.taxi_zone_raw
stage.taxi_zone_geometry
stage.yellow_taxi_raw
stage.yellow_taxi_canonical
dq.technical_quarantine
```

The table and index migrations were executed twice with `ON_ERROR_STOP=1`. The second execution completed idempotently using `IF NOT EXISTS`.

## SQL test results

```text
PASS: task 4.5 table and canonical schema contract
PASS: task 4.5 constraints and source-key uniqueness
PASS: task 4.5 required indexes
```

Validated items:

- All five relations exist.
- Canonical table exposes all 33 approved contract columns.
- Geometry is MultiPolygon SRID 2263.
- Required FK and unique constraints exist.
- Duplicate source record keys are rejected.
- All 15 required secondary indexes exist.
- Geometry index is GiST and cast-error JSON index is GIN.

## Regression

The existing task 4.4 Mage unit/integration suite was rerun after updating the stage loader:

```text
Ran 11 tests
OK
```

A live PostgreSQL smoke test sent the same raw DataFrame twice through
`upsert_dataframe`. Target-type casts succeeded, including text to timestamptz,
and `ON CONFLICT (source_record_key) DO NOTHING` retained exactly one row. The
test row and temporary control batch were removed after the assertion.

## Remaining integration gate

No source data was loaded during the original schema/index checkpoint. The later
reference load is recorded below; one-month Yellow Taxi, reconciliation and
rerun evidence remain required before declaring tasks 4.4 and 4.5 complete
end-to-end.

## Reference load update — 2026-11-18

After preserving TLC literal `N/A` values during CSV parsing, the reference
pipeline loaded 265 lookup rows and 263 EPSG:2263 geometries. The only lookup
members without polygons are the expected LocationID 264 and 265. Yellow Taxi
subset, reconciliation and rerun evidence remain pending.
