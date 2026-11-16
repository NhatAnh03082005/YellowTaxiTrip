"""Build deterministic, commit-safe fixtures from validated Yellow Taxi files."""

from __future__ import annotations

import csv
import hashlib
import json
import random
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = PROJECT_ROOT / "tests" / "fixtures"
VALID_ROOT = FIXTURE_ROOT / "valid"
INVALID_ROOT = FIXTURE_ROOT / "invalid"
EXPECTED_ROOT = PROJECT_ROOT / "tests" / "expected_results"
CATALOG_PATH = PROJECT_ROOT / "data" / "catalog" / "yellow_taxi_2024_2025.csv"
SEED = 20261116
TARGET_RANDOM_ROWS = 5_000
SOURCE_MONTHS = ("2024-05", "2025-05")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def random_row_group_sample(parquet: pq.ParquetFile, seed: int) -> pa.Table:
    rng = random.Random(seed)
    row_group_count = parquet.metadata.num_row_groups
    base = TARGET_RANDOM_ROWS // row_group_count
    remainder = TARGET_RANDOM_ROWS % row_group_count
    parts: list[pa.Table] = []
    for row_group_index in range(row_group_count):
        table = parquet.read_row_group(row_group_index)
        requested = base + (1 if row_group_index < remainder else 0)
        requested = min(requested, table.num_rows)
        indices = sorted(rng.sample(range(table.num_rows), requested))
        parts.append(table.take(pa.array(indices, type=pa.int64())))
    return pa.concat_tables(parts)


def condition_masks(table: pa.Table, has_cbd: bool) -> dict[str, pa.Array]:
    passenger = table["passenger_count"]
    distance = table["trip_distance"]
    total = table["total_amount"]
    payment = table["payment_type"]
    rate = table["RatecodeID"]
    pu = table["PULocationID"]
    do = table["DOLocationID"]
    masks = {
        "passenger_null": pc.is_null(passenger),
        "passenger_zero_or_gt6": pc.or_(
            pc.less_equal(pc.fill_null(passenger, 1), 0),
            pc.greater(pc.fill_null(passenger, 1), 6),
        ),
        "zone_264_or_265": pc.or_(
            pc.is_in(pu, value_set=pa.array([264, 265], type=pu.type)),
            pc.is_in(do, value_set=pa.array([264, 265], type=do.type)),
        ),
        "non_positive_total": pc.less_equal(pc.fill_null(total, 1.0), 0),
        "non_positive_distance": pc.less_equal(pc.fill_null(distance, 1.0), 0),
        "business_unknown_or_flex_code": pc.or_(
            pc.is_in(rate, value_set=pa.array([99], type=rate.type)),
            pc.is_in(payment, value_set=pa.array([0, 5], type=payment.type)),
        ),
    }
    if has_cbd:
        cbd = table["cbd_congestion_fee"]
        masks["positive_cbd_fee"] = pc.greater(pc.fill_null(cbd, 0.0), 0)
    return masks


def coverage_counts(table: pa.Table) -> dict[str, int]:
    return {
        name: len(pc.indices_nonzero(pc.fill_null(mask, False)))
        for name, mask in condition_masks(table, "cbd_congestion_fee" in table.column_names).items()
    }


def find_missing_edge_rows(parquet: pq.ParquetFile, sample: pa.Table) -> list[pa.Table]:
    coverage = coverage_counts(sample)
    missing = {name for name, count in coverage.items() if count == 0}
    found: list[pa.Table] = []
    if not missing:
        return found
    has_cbd = "cbd_congestion_fee" in parquet.schema_arrow.names
    for batch in parquet.iter_batches(batch_size=65_536):
        table = pa.Table.from_batches([batch])
        masks = condition_masks(table, has_cbd)
        for name in list(missing):
            indices = pc.indices_nonzero(pc.fill_null(masks[name], False))
            if len(indices):
                found.append(table.slice(indices[0].as_py(), 1))
                missing.remove(name)
        if not missing:
            break
    if missing:
        raise RuntimeError(f"Could not find required edge cases: {sorted(missing)}")
    return found


def build_valid_fixture(month: str, catalog_by_month: dict[str, dict[str, str]]) -> dict[str, object]:
    source_path = (
        PROJECT_ROOT
        / "data"
        / "yellow_taxi"
        / "by_year_month"
        / month
        / f"yellow_tripdata_{month}.parquet"
    )
    parquet = pq.ParquetFile(source_path)
    sample = random_row_group_sample(parquet, SEED + int(month.replace("-", "")))
    edge_rows = find_missing_edge_rows(parquet, sample)
    if edge_rows:
        sample = pa.concat_tables([sample, *edge_rows])
    output_path = VALID_ROOT / f"yellow_tripdata_{month}_sample.parquet"
    pq.write_table(sample, output_path, compression="zstd")
    return {
        "fixture": output_path.relative_to(PROJECT_ROOT).as_posix(),
        "source_month": month,
        "source_file": source_path.name,
        "source_sha256": catalog_by_month[month]["sha256"],
        "selection_seed": SEED + int(month.replace("-", "")),
        "random_rows": TARGET_RANDOM_ROWS,
        "total_fixture_rows": sample.num_rows,
        "coverage": coverage_counts(sample),
        "fixture_sha256": sha256_file(output_path),
    }


def build_business_invalid_fixture() -> dict[str, object]:
    source_path = VALID_ROOT / "yellow_tripdata_2025-05_sample.parquet"
    source = pq.read_table(source_path).slice(0, 9)
    records = source.to_pylist()
    cases = [
        ("VALID_CONTROL", "NONE", "Unmodified control row"),
        ("DROPOFF_BEFORE_PICKUP", "TIME-002", "Dropoff is one minute before pickup"),
        ("NEGATIVE_DISTANCE", "DIST-001", "trip_distance = -1"),
        ("ZERO_PASSENGER", "PASS-001", "passenger_count = 0"),
        ("ZERO_TOTAL", "AMT-001", "total_amount = 0"),
        ("VOIDED_PAYMENT", "CODE-003", "payment_type = 6"),
        ("NULL_PICKUP_ZONE", "LOC-001", "PULocationID is null"),
        ("IMPOSSIBLE_SPEED", "SPEED-002", "100 miles in ten minutes"),
        ("RAW_EXACT_DUPLICATE_WINNER", "NONE", "First occurrence of exact row"),
    ]
    records[1]["tpep_dropoff_datetime"] = records[1]["tpep_pickup_datetime"] - timedelta(minutes=1)
    records[2]["trip_distance"] = -1.0
    records[3]["passenger_count"] = 0
    records[4]["total_amount"] = 0.0
    records[5]["payment_type"] = 6
    records[6]["PULocationID"] = None
    records[7]["trip_distance"] = 100.0
    records[7]["tpep_dropoff_datetime"] = records[7]["tpep_pickup_datetime"] + timedelta(minutes=10)
    records[8] = records[0].copy()
    cases[8] = (
        "RAW_EXACT_DUPLICATE_LOSER",
        "DUP-001",
        "Exact duplicate of fixture row 1; later source row loses",
    )
    invalid_table = pa.Table.from_pylist(records, schema=source.schema)
    output_path = INVALID_ROOT / "business_rule_cases.parquet"
    pq.write_table(invalid_table, output_path, compression="zstd")
    expected_path = EXPECTED_ROOT / "business_rule_cases_expected.csv"
    with expected_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["fixture_row_number", "case_id", "expected_rule_id", "description"])
        for index, case in enumerate(cases, start=1):
            writer.writerow([index, *case])
    return {
        "fixture": output_path.relative_to(PROJECT_ROOT).as_posix(),
        "rows": invalid_table.num_rows,
        "fixture_sha256": sha256_file(output_path),
        "expected_results": expected_path.relative_to(PROJECT_ROOT).as_posix(),
    }


def build_technical_cast_fixture() -> dict[str, object]:
    output_path = INVALID_ROOT / "technical_cast_errors.csv"
    rows = [
        ["BAD_PICKUP", "not-a-timestamp", "2025-05-01 10:15:00", "132", "138", "25.50", "CAST-001"],
        ["BAD_DROPOFF", "2025-05-01 10:00:00", "invalid", "132", "138", "25.50", "CAST-001"],
        ["BAD_PU", "2025-05-01 10:00:00", "2025-05-01 10:15:00", "ABC", "138", "25.50", "CAST-002"],
        ["BAD_DO", "2025-05-01 10:00:00", "2025-05-01 10:15:00", "132", "?", "25.50", "CAST-002"],
        ["BAD_TOTAL", "2025-05-01 10:00:00", "2025-05-01 10:15:00", "132", "138", "USD25", "CAST-003"],
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(
            [
                "case_id",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "total_amount",
                "expected_rule_id",
            ]
        )
        writer.writerows(rows)
    return {
        "fixture": output_path.relative_to(PROJECT_ROOT).as_posix(),
        "rows": len(rows),
        "fixture_sha256": sha256_file(output_path),
    }


def main() -> None:
    VALID_ROOT.mkdir(parents=True, exist_ok=True)
    INVALID_ROOT.mkdir(parents=True, exist_ok=True)
    EXPECTED_ROOT.mkdir(parents=True, exist_ok=True)
    with CATALOG_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        catalog_by_month = {row["source_year_month"]: row for row in csv.DictReader(handle)}
    valid = [build_valid_fixture(month, catalog_by_month) for month in SOURCE_MONTHS]
    manifest = {
        "generation_contract_date": "2026-11-16",
        "generator": "profiling/scripts/build_test_fixtures.py",
        "selection_policy": "Seeded random sample across every Parquet row group plus required real edge cases",
        "valid_fixtures": valid,
        "invalid_business_fixture": build_business_invalid_fixture(),
        "invalid_technical_fixture": build_technical_cast_fixture(),
    }
    manifest_path = FIXTURE_ROOT / "fixture_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
