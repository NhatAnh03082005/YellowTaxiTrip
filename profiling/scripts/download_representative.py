from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import requests

from common import PROFILES, RAW, init_dirs, logger, sha256

YEARS = [2009, 2010, 2015, 2019, 2024, 2025, 2026]


def download(url: str, target: Path, log):
    if target.exists() and target.stat().st_size > 0:
        log.info("Reuse %s", target); return
    tmp = target.with_suffix(target.suffix + ".part")
    with requests.get(url, stream=True, timeout=(30, 300), headers={"User-Agent":"NYC-Taxi-Survey/1.0"}) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(8 * 1024 * 1024):
                if chunk: f.write(chunk)
    tmp.replace(target); log.info("Downloaded %s bytes to %s", target.stat().st_size, target)


def main():
    init_dirs(); log = logger("download_representative")
    mf = pd.read_csv(PROFILES / "01_yellow_taxi_file_manifest.csv")
    selected=[]
    for y in YEARS:
        cand = mf[(mf.year == y) & mf.url.notna() & mf.url.ne("") & mf.access_status.isin(["ok", "not_checked"])].sort_values("month")
        jan = cand[cand.month == 1]
        row = (jan.iloc[0] if len(jan) else cand.iloc[0]).to_dict()
        row["selection_reason"] = "January available" if row["month"] == 1 else "January unavailable; nearest available month selected"
        target = RAW / row["file_name"]
        download(row["url"], target, log)
        row["local_path"] = str(target.relative_to(target.parents[2])); row["downloaded_size_bytes"] = target.stat().st_size; row["sha256"] = sha256(target)
        selected.append(row)
    pd.DataFrame(selected).to_csv(PROFILES / "representative_files.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__": main()
