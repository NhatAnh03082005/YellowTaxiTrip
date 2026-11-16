from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from common import CHECKED_AT, PROFILES, TLC_PAGE, init_dirs, logger

PATTERN = re.compile(r"yellow_tripdata_(20\d{2})-(\d{2})\.(parquet|csv)$", re.I)


def head(session, url):
    try:
        r = session.head(url, allow_redirects=True, timeout=30)
        if r.status_code in (403, 405):
            r = session.get(url, headers={"Range": "bytes=0-0"}, stream=True, timeout=30)
        return r.status_code, int(r.headers.get("content-length", 0) or 0), r.url
    except requests.RequestException:
        return 0, 0, url


def main(check_links=True):
    init_dirs(); log = logger("crawl_tlc_manifest")
    s = requests.Session(); s.headers["User-Agent"] = "NYC-Taxi-Survey/1.0 academic data profiling"
    html = s.get(TLC_PAGE, timeout=60); html.raise_for_status()
    soup = BeautifulSoup(html.text, "lxml")
    found = {}
    for a in soup.select("a[href]"):
        url = urljoin(TLC_PAGE, a["href"])
        m = PATTERN.search(url.split("?")[0])
        if m:
            y, mo, ext = int(m.group(1)), int(m.group(2)), m.group(3).lower()
            if 2009 <= y <= 2026:
                found[(y, mo)] = (url, ext)
    checks={}
    if check_links:
        urls=[u for u,_ in found.values()]
        with ThreadPoolExecutor(max_workers=16) as ex:
            checks=dict(zip(urls,ex.map(lambda u: head(s,u),urls)))
    rows = []
    for y in range(2009, 2027):
        for mo in range(1, 13):
            item = found.get((y, mo))
            if not item:
                rows.append(dict(year=y, month=mo, file_name="", url="", format="", size_bytes="", http_status="", access_status="missing_from_page", checked_at=CHECKED_AT, duplicate_url=False, zero_size=False, name_valid=False))
                continue
            url, ext = item; name = url.rsplit("/", 1)[-1]
            status, size, final = checks[url] if check_links else ("", "", url)
            rows.append(dict(year=y, month=mo, file_name=name, url=url, format=ext, size_bytes=size, http_status=status, access_status="ok" if status in (200,206) else ("not_checked" if status == "" else "broken"), checked_at=CHECKED_AT, duplicate_url=False, zero_size=(size == 0 if status else False), name_valid=bool(PATTERN.fullmatch(name))))
    df = pd.DataFrame(rows)
    present = df.url.ne("")
    df.loc[present, "duplicate_url"] = df.loc[present, "url"].duplicated(keep=False)
    path = PROFILES / "01_yellow_taxi_file_manifest.csv"; df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("Wrote %s (%s linked months, %s missing page slots)", path, present.sum(), (~present).sum())


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--no-head", action="store_true")
    args = ap.parse_args(); main(not args.no_head)
