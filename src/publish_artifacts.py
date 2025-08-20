# -*- coding: utf-8 -*-
"""
Publish staged outputs into data/{latest, daily_snapshots, history}.

Modes:
  - latest: copy ONLY to data/latest/
  - daily : copy to data/latest/ AND archive in data/daily_snapshots/ and append to data/history/
"""
import argparse, os, shutil, csv
from typing import List, Dict
from .common.io_utils import ensure_dir, append_history_rows
from .common.schema import CSV_FIELDS

def read_csv_rows(path: str) -> List[Dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append(r)
    return rows

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", required=True)            # tmp/YYYYMMDD/
    ap.add_argument("--repo-root", default=".")
    ap.add_argument("--date", required=True)               # YYYY-MM-DD
    ap.add_argument("--mode", choices=["latest", "daily"], default="daily")
    args = ap.parse_args(argv)

    ymd = args.date.replace("-", "")
    latest_dir = os.path.join(args.repo_root, "data", "latest")
    daily_dir  = os.path.join(args.repo_root, "data", "daily_snapshots")
    hist_dir   = os.path.join(args.repo_root, "data", "history")

    ensure_dir(latest_dir)
    if args.mode == "daily":
        ensure_dir(daily_dir)
        ensure_dir(hist_dir)

    # ---- always update data/latest/ ----
    for name in ("drift_latest.json", "hyperliquid_latest.json", "dydx_latest.json", "all_latest.csv", "all_latest.json"):
        src = os.path.join(args.staging, name)
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(latest_dir, name))

    if args.mode == "latest":
        print("[publish] latest-only updated.")
        return 0

    # ---- daily: archive per-day files ----
    mapping = {
        "drift_latest.json":        f"drift_{ymd}.json",
        "hyperliquid_latest.json":  f"hyperliquid_{ymd}.json",
        "dydx_latest.json":         f"dydx_{ymd}.json",
        "all_latest.csv":           f"all_{ymd}.csv",
    }
    for src_name, dst_name in mapping.items():
        src = os.path.join(args.staging, src_name)
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(daily_dir, dst_name))

    # ---- daily: append to yearly history ----
    latest_csv = os.path.join(args.staging, "all_latest.csv")
    if os.path.exists(latest_csv):
        rows = read_csv_rows(latest_csv)
        append_history_rows(hist_dir, snapshot_date=args.date, rows=rows)

    print("[publish] daily updated: latest/, daily_snapshots/, history/")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
