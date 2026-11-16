"""Download, validate, fingerprint, and catalog Yellow Taxi ETL inputs.

The downloader is resumable: partial content stays in ``*.tmp`` and completed
Parquet files are revalidated instead of downloaded again. Catalog updates are
written atomically only after all requested files have been processed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import requests


CATALOG_COLUMNS = [
    "source_year_month",
    "file_name",
    "source_url",
    "http_status",
    "content_length",
    "last_modified",
    "sha256",
    "schema_fingerprint",
    "schema_version",
    "local_path",
    "downloaded_at",
    "validation_status",
]
REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pulocationid",
    "dolocationid",
    "total_amount",
}
CHUNK_SIZE = 8 * 1024 * 1024


def replace_generated_file(temporary_path: Path, destination_path: Path) -> None:
    """Replace a generated CSV, with a Windows-safe fallback for existing files."""
    try:
        os.replace(temporary_path, destination_path)
    except PermissionError:
        # Some Windows filesystems reject atomic replacement of an existing file
        # even when it is writable. The fully written temporary file remains the
        # source of truth, so copy it over and remove it only after success.
        shutil.copyfile(temporary_path, destination_path)
        temporary_path.unlink()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path("data/catalog/yellow_taxi_2024_2025.csv"),
    )
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=120)
    return parser.parse_args()


def read_catalog(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != CATALOG_COLUMNS:
            raise ValueError(f"Unexpected catalog columns: {reader.fieldnames}")
        rows = list(reader)
    if len(rows) != 24:
        raise ValueError(f"Expected 24 catalog rows, found {len(rows)}")
    months = [row["source_year_month"] for row in rows]
    if len(set(months)) != 24:
        raise ValueError("source_year_month must be unique")
    return rows


def atomic_write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="", delete=False, dir=path.parent, suffix=".tmp"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=CATALOG_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        temporary_path = Path(handle.name)
    replace_generated_file(temporary_path, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def validate_parquet(path: Path) -> tuple[int, str, str]:
    parquet = pq.ParquetFile(path)
    row_count = parquet.metadata.num_rows
    if row_count <= 0:
        raise ValueError("Parquet contains no rows")
    schema = parquet.schema_arrow
    lower_names = {name.lower() for name in schema.names}
    missing = sorted(REQUIRED_COLUMNS - lower_names)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    schema_contract = [
        {"ordinal": index, "name": field.name, "logical_type": str(field.type)}
        for index, field in enumerate(schema)
    ]
    serialized = json.dumps(schema_contract, ensure_ascii=False, separators=(",", ":"))
    fingerprint = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return row_count, fingerprint, serialized


def head_metadata(session: requests.Session, url: str, timeout: int) -> dict[str, Any]:
    response = session.head(url, allow_redirects=True, timeout=timeout)
    response.raise_for_status()
    length_text = response.headers.get("Content-Length", "")
    return {
        "http_status": response.status_code,
        "content_length": int(length_text) if length_text.isdigit() else 0,
        "last_modified": response.headers.get("Last-Modified", ""),
    }


def download_with_resume(
    session: requests.Session,
    url: str,
    temporary_path: Path,
    expected_length: int,
    timeout: int,
) -> tuple[int, str]:
    temporary_path.parent.mkdir(parents=True, exist_ok=True)
    existing_size = temporary_path.stat().st_size if temporary_path.exists() else 0
    headers: dict[str, str] = {}
    mode = "wb"
    if existing_size and (not expected_length or existing_size < expected_length):
        headers["Range"] = f"bytes={existing_size}-"
        mode = "ab"
    elif expected_length and existing_size > expected_length:
        existing_size = 0
    response = session.get(url, headers=headers, stream=True, timeout=timeout)
    response.raise_for_status()
    if mode == "ab" and response.status_code != 206:
        mode = "wb"
        existing_size = 0
    with temporary_path.open(mode) as handle:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                handle.write(chunk)
    final_size = temporary_path.stat().st_size
    if expected_length and final_size != expected_length:
        raise ValueError(f"Content length mismatch: expected {expected_length}, got {final_size}")
    return final_size, response.headers.get("Last-Modified", "")


def process_row(row: dict[str, str], project_root: Path, timeout: int) -> dict[str, Any]:
    final_path = project_root / Path(row["local_path"])
    temporary_path = final_path.with_name(final_path.name + ".tmp")
    with requests.Session() as session:
        session.headers.update({"User-Agent": "NYC-Taxi-BI-ETL/1.0"})
        head = head_metadata(session, row["source_url"], timeout)
        expected_length = int(head["content_length"])
        downloaded_now = False
        if not final_path.exists():
            size, get_last_modified = download_with_resume(
                session, row["source_url"], temporary_path, expected_length, timeout
            )
            row_count, fingerprint, schema_contract = validate_parquet(temporary_path)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(temporary_path, final_path)
            downloaded_now = True
            if not head["last_modified"]:
                head["last_modified"] = get_last_modified
        else:
            size = final_path.stat().st_size
            if expected_length and size != expected_length:
                raise ValueError(
                    f"Existing file size mismatch for {final_path}: expected {expected_length}, got {size}"
                )
            row_count, fingerprint, schema_contract = validate_parquet(final_path)
        digest = sha256_file(final_path)
        return {
            "source_year_month": row["source_year_month"],
            "http_status": str(head["http_status"]),
            "content_length": str(size),
            "last_modified": head["last_modified"],
            "sha256": digest,
            "schema_fingerprint": fingerprint,
            "schema_contract": schema_contract,
            "row_count": row_count,
            "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "downloaded_now": downloaded_now,
        }


def load_registry(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_registry(path: Path, registry: list[dict[str, str]]) -> None:
    fields = ["schema_version", "schema_fingerprint", "first_seen_month", "schema_contract_json"]
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="", delete=False, dir=path.parent, suffix=".tmp"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(registry)
        temporary_path = Path(handle.name)
    replace_generated_file(temporary_path, path)


def main() -> int:
    args = parse_args()
    project_root = args.project_root.resolve()
    catalog_path = args.catalog
    if not catalog_path.is_absolute():
        catalog_path = project_root / catalog_path
    rows = read_catalog(catalog_path)
    results: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(process_row, row, project_root, args.timeout): row["source_year_month"]
            for row in rows
        }
        for future in as_completed(futures):
            month = futures[future]
            try:
                result = future.result()
                results[month] = result
                action = "downloaded" if result["downloaded_now"] else "revalidated"
                print(
                    f"[OK] {month}: {action}; rows={result['row_count']:,}; "
                    f"bytes={int(result['content_length']):,}; sha256={result['sha256'][:12]}...",
                    flush=True,
                )
            except Exception as exc:  # report all file failures together
                errors[month] = f"{type(exc).__name__}: {exc}"
                print(f"[ERROR] {month}: {errors[month]}", flush=True)
    if errors:
        print(f"Validation failed for {len(errors)} file(s): {errors}")
        return 1

    registry_path = catalog_path.parent / "yellow_taxi_schema_registry.csv"
    registry = load_registry(registry_path)
    fingerprint_to_version = {
        item["schema_fingerprint"]: item["schema_version"] for item in registry
    }
    next_version = len(registry) + 1
    for row in sorted(rows, key=lambda item: item["source_year_month"]):
        result = results[row["source_year_month"]]
        fingerprint = result["schema_fingerprint"]
        if fingerprint not in fingerprint_to_version:
            version = f"yt_schema_v{next_version:03d}"
            fingerprint_to_version[fingerprint] = version
            registry.append(
                {
                    "schema_version": version,
                    "schema_fingerprint": fingerprint,
                    "first_seen_month": row["source_year_month"],
                    "schema_contract_json": result["schema_contract"],
                }
            )
            next_version += 1
    for row in rows:
        result = results[row["source_year_month"]]
        row.update(
            {
                "http_status": result["http_status"],
                "content_length": result["content_length"],
                "last_modified": result["last_modified"],
                "sha256": result["sha256"],
                "schema_fingerprint": result["schema_fingerprint"],
                "schema_version": fingerprint_to_version[result["schema_fingerprint"]],
                "downloaded_at": result["downloaded_at"],
                "validation_status": "VALIDATED",
            }
        )
    write_registry(registry_path, registry)
    atomic_write_csv(catalog_path, rows)
    print(
        f"Catalog updated: {len(rows)} files VALIDATED; "
        f"{len(registry)} registered schema version(s)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
