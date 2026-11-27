"""Run dbt build and print a readable result for every test.

Failure tables are opt-in for the small, aggregate DDS test selectors only.
The dbt exit code is preserved.
"""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


def failure_rows(relation_name, limit):
    """Return stored failing rows, or a short reason they cannot be read."""
    try:
        import psycopg2
        from psycopg2 import sql
    except ImportError:
        return None, "psycopg2 is unavailable; query the stored relation manually"

    names = re.findall(r'"((?:[^"]|"")*)"', relation_name or "")
    if len(names) < 2:
        return None, "dbt did not provide a stored-failure relation"
    schema, table = (name.replace('""', '"') for name in names[-2:])
    required = ("DBT_HOST", "DBT_PORT", "POSTGRES_USER", "POSTGRES_DB")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        return None, "database variables missing: " + ", ".join(missing)

    try:
        with psycopg2.connect(
            host=os.environ["DBT_HOST"],
            port=os.environ["DBT_PORT"],
            user=os.environ["POSTGRES_USER"],
            password=os.getenv("POSTGRES_PASSWORD", ""),
            dbname=os.environ["POSTGRES_DB"],
            connect_timeout=5,
        ) as connection:
            with connection.cursor() as cursor:
                query = sql.SQL("select * from {} limit %s").format(
                    sql.Identifier(schema, table)
                )
                cursor.execute(query, (limit,))
                columns = [column.name for column in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return rows, None
    except Exception as exc:
        return None, f"could not read stored failures: {exc.__class__.__name__}: {exc}"


def report(target_path, max_failure_rows, started_at=None):
    result_path = target_path / "run_results.json"
    manifest_path = target_path / "manifest.json"
    if not result_path.exists() or not manifest_path.exists():
        print("No dbt run_results.json and manifest.json found; no test report available.")
        return
    if started_at is not None and result_path.stat().st_mtime < started_at - 1:
        print("dbt did not write new test results; an older report was ignored.")
        return

    results = json.loads(result_path.read_text(encoding="utf-8"))["results"]
    nodes = json.loads(manifest_path.read_text(encoding="utf-8"))["nodes"]
    tests = [item for item in results if item["unique_id"].startswith("test.")]
    print(f"\nDETAILED TEST RESULTS ({len(tests)} tests)")
    for index, item in enumerate(tests, start=1):
        node = nodes.get(item["unique_id"], {})
        name = node.get("name", item["unique_id"])
        status = item.get("status", "unknown").upper()
        failures = item.get("failures")
        duration = item.get("execution_time", 0)
        path = node.get("original_file_path", "")
        print(f"[{index}/{len(tests)}] {status} {name} | failures={failures} "
              f"| {duration:.2f}s | {path}")
        if status not in ("PASS", "WARN"):
            if item.get("message"):
                print("  Detail:", item["message"])
            relation = item.get("relation_name")
            if relation:
                print("  Stored failures:", relation)
                rows, error = failure_rows(relation, max_failure_rows)
                if error:
                    print("  ", error)
                else:
                    for row in rows:
                        trimmed = {
                            key: str(value)[:200] if value is not None else None
                            for key, value in row.items()
                        }
                        print("  Row:", json.dumps(trimmed, ensure_ascii=False))
                    if (failures or 0) > len(rows):
                        print(f"  Showing {len(rows)} of {failures} failing rows")
    counts = {}
    for item in tests:
        status = item.get("status", "unknown").upper()
        counts[status] = counts.get(status, 0) + 1
    print("TEST SUMMARY:", ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dbt-executable", default=shutil.which("dbt"))
    parser.add_argument("--project-dir", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--target-path", type=Path)
    parser.add_argument("--max-failure-rows", type=int, default=30)
    parser.add_argument("--store-failures", action="store_true",
                        help="Store only aggregate DDS selector failures for detailed rule output")
    parser.add_argument("--report-only", action="store_true")
    options, dbt_args = parser.parse_known_args()
    if options.max_failure_rows < 1:
        parser.error("--max-failure-rows must be positive")
    project_dir = options.project_dir.resolve()
    target_path = (options.target_path or project_dir / "target").resolve()
    if options.report_only:
        report(target_path, options.max_failure_rows)
        return 0
    if not options.dbt_executable:
        parser.error("dbt executable not found; use --dbt-executable")
    if options.store_failures:
        selected = [dbt_args[i + 1] for i, value in enumerate(dbt_args[:-1])
                    if value == "--selector"]
        if (selected not in (["minimal_dds_batch_tests"], ["minimal_dds_full_audit"])
                or any(value in dbt_args for value in ("--select", "-s", "--exclude"))):
            parser.error("--store-failures requires --selector minimal_dds_batch_tests or minimal_dds_full_audit")

    command = [options.dbt_executable, "build",
               "--project-dir", str(project_dir), "--target-path", str(target_path),
               *(["--store-failures"] if options.store_failures else []),
               *dbt_args]
    started_at = time.time()
    completed = subprocess.run(command, check=False)
    report(target_path, options.max_failure_rows, started_at)
    return completed.returncode


if __name__ == "__main__":
    sys.exit(main())
