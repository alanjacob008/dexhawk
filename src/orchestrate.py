# -*- coding: utf-8 -*-
"""
Orchestrator: run collectors -> combine -> publish.
"""
import argparse, datetime as dt, os, subprocess, sys

def run(cmd: list):
    print("[exec]", " ".join(cmd))
    r = subprocess.run(cmd, check=False)
    if r.returncode != 0:
        print(f"[warn] step failed rc={r.returncode}")
    return r.returncode

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=dt.datetime.utcnow().date().isoformat())  # UTC
    args = ap.parse_args(argv)

    ymd = args.date.replace("-", "")
    staging = os.path.join("tmp", ymd)
    os.makedirs(staging, exist_ok=True)

    # 1) collectors
    rc1 = run([sys.executable, "-m", "src.dydx_collect",
               "--out", os.path.join(staging, "dydx_latest.json"),
               "--daily-snapshot", args.date,
               "--indexer", "https://indexer.dydx.trade",
               "--symbols-out", "symbol_registry/dydx_symbols.json"])

    rc2 = run([sys.executable, "-m", "src.drift_collect",
               "--out", os.path.join(staging, "drift_latest.json"),
               "--daily-snapshot", args.date,
               "--symbols-out", "symbol_registry/drift_symbols.json"])

    rc3 = run([sys.executable, "-m", "src.hl_collect",
               "--out", os.path.join(staging, "hyperliquid_latest.json"),
               "--daily-snapshot", args.date,
               "--symbols-out", "symbol_registry/hyperliquid_symbols.json"])

    # 2) combine (placeholders used automatically if a file missing/invalid)
    run([sys.executable, "-m", "src.combine_daily",
         "--drift", os.path.join(staging, "drift_latest.json"),
         "--hl",    os.path.join(staging, "hyperliquid_latest.json"),
         "--dydx",  os.path.join(staging, "dydx_latest.json"),
         "--out-csv",  os.path.join(staging, "all_latest.csv"),
         "--out-json", os.path.join(staging, "all_latest.json"),
         "--daily-snapshot", args.date])

    # 3) publish
    run([sys.executable, "-m", "src.publish_artifacts",
         "--staging", staging,
         "--repo-root", ".",
         "--date", args.date])

    print(f"[done] snapshot {args.date}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
