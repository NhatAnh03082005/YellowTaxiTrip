"""Validate committed fixtures without requiring the large source Parquet files."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = PROJECT_ROOT / "tests" / "fixtures"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    manifest_path = FIXTURE_ROOT / "fixture_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in manifest["valid_fixtures"]:
        path = PROJECT_ROOT / item["fixture"]
        require(path.is_file(), f"Missing fixture: {path}")
        parquet = pq.ParquetFile(path)
        require(parquet.metadata.num_rows == item["total_fixture_rows"], f"Row mismatch: {path}")
        require(sha256_file(path) == item["fixture_sha256"], f"SHA-256 mismatch: {path}")
        require(all(count > 0 for count in item["coverage"].values()), f"Coverage gap: {path}")
        has_cbd = "cbd_congestion_fee" in parquet.schema_arrow.names
        require(has_cbd == (item["source_month"] == "2025-05"), f"CBD schema mismatch: {path}")
    business = manifest["invalid_business_fixture"]
    business_path = PROJECT_ROOT / business["fixture"]
    require(pq.ParquetFile(business_path).metadata.num_rows == business["rows"], "Business row mismatch")
    require(sha256_file(business_path) == business["fixture_sha256"], "Business fixture hash mismatch")
    with (PROJECT_ROOT / business["expected_results"]).open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        expected = list(csv.DictReader(handle))
    require(len(expected) == business["rows"], "Business expected-result count mismatch")
    require(any(row["expected_rule_id"] == "DUP-001" for row in expected), "DUP-001 case missing")
    technical = manifest["invalid_technical_fixture"]
    technical_path = PROJECT_ROOT / technical["fixture"]
    require(sha256_file(technical_path) == technical["fixture_sha256"], "Technical fixture hash mismatch")
    with technical_path.open("r", encoding="utf-8-sig", newline="") as handle:
        technical_rows = list(csv.DictReader(handle))
    require(len(technical_rows) == technical["rows"], "Technical fixture row mismatch")
    print("Fixture validation passed")


if __name__ == "__main__":
    main()
