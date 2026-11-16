from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW = DATA / "raw"
REF = DATA / "reference"
SAMPLE = DATA / "sample"
OUT = ROOT / "outputs"
PROFILES = OUT / "profiles"
ANOMALIES = OUT / "anomalies"
MAPPINGS = OUT / "mappings"
REPORTS = OUT / "reports"
LOGS = ROOT / "logs"
CHECKED_AT = "2026-10-30"
TLC_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"


def init_dirs() -> None:
    for p in (RAW, REF, SAMPLE, PROFILES, ANOMALIES, MAPPINGS, REPORTS, LOGS):
        p.mkdir(parents=True, exist_ok=True)


def logger(name: str) -> logging.Logger:
    init_dirs()
    log = logging.getLogger(name)
    if not log.handlers:
        log.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh = logging.FileHandler(LOGS / f"{name}.log", encoding="utf-8")
        sh = logging.StreamHandler()
        fh.setFormatter(fmt); sh.setFormatter(fmt)
        log.addHandler(fh); log.addHandler(sh)
    return log


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
