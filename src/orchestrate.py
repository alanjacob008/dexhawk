# -*- coding: utf-8 -*-
"""
Orchestrator: run collectors -> combine -> publish.

Usage:
  # daily snapshot (default)
  python -m src.orchestrate

  # daily for a specific UTC date
  python -m src.orchestrate --date 2025-08-20

  # latest-only (hourly job): updates data/latest/ only
  python -m src.orchestrate --mode latest
"""
import argparse, datetime as dt, os, subprocess, sys

# ---- small runner helper ----
def run(cmd: list) -> int:
    print("[exec]", " ".join(cmd))
    r = subprocess.run(cmd, check=False)
    if r.returncode != 0:
        print(f"[warn] step failed rc={r.returncode}")
    return r.returncode

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=dt.datetime.utcnow().date().isoformat(), help="UTC date YYYY-MM-DD")
    ap.add_argument("--mode", choices=["daily", "latest"], default="daily", help="daily: write latest+daily+history; latest: write latest only")
    args = ap.parse_args(argv)

    ymd = args.date.replace("-", "")
    staging = os.path.join("tmp", ymd)
    os.makedirs(staging, exist_ok=True)

    # 1) collectors (always run, both modes)
    run([sys.executable, "-m", "src.dydx_collect",
         "--out", os.path.join(staging, "dydx_latest.json"),
         "--daily-snapshot", args.date,
         "--indexer", "https://indexer.dydx.trade",
         "--symbols-out", "symbol_registry/dydx_symbols.json"])

    run([sys.executable, "-m", "src.drift_collect",
         "--out", os.path.join(staging, "drift_latest.json"),
         "--daily-snapshot", args.date,
         "--symbols-out", "symbol_registry/drift_symbols.json"])

    run([sys.executable, "-m", "src.hl_collect",
         "--out", os.path.join(staging, "hyperliquid_latest.json"),
         "--daily-snapshot", args.date,
         "--symbols-out", "symbol_registry/hyperliquid_symbols.json"])

    # 2) combine (placeholders kick in if any collector failed)
    run([sys.executable, "-m", "src.combine_daily",
         "--drift", os.path.join(staging, "drift_latest.json"),
         "--hl",    os.path.join(staging, "hyperliquid_latest.json"),
         "--dydx",  os.path.join(staging, "dydx_latest.json"),
         "--out-csv",  os.path.join(staging, "all_latest.csv"),
         "--out-json", os.path.join(staging, "all_latest.json"),
         "--daily-snapshot", args.date])

    # 3) publish (switch behavior by mode)
    run([sys.executable, "-m", "src.publish_artifacts",
         "--staging", staging,
         "--repo-root", ".",
         "--date", args.date,
         "--mode", args.mode])

    print(f"[done] {args.mode} run for {args.date}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
