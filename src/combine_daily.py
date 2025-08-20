# -*- coding: utf-8 -*-
"""
Combine venue JSONs -> daily CSV (+optional JSON).
"""
import argparse, csv, json, os
from typing import List, Dict
from .common.io_utils import read_json, read_symbol_registry
from .common.placeholders import make_placeholders
from .common.schema import CSV_FIELDS

def load_rows_or_placeholders(path: str, base_dir: str, exchange: str, date_str: str) -> List[Dict]:
    if os.path.exists(path):
        try:
            data = read_json(path)
            if isinstance(data, list):
                # ensure daily_snapshot exists on rows
                for r in data:
                    r.setdefault("daily_snapshot", date_str)
                return data
        except Exception:
            pass
    # fallback to placeholders using registry
    syms = read_symbol_registry(base_dir, exchange)
    return make_placeholders(exchange, syms, date_str)

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--drift", required=True)
    ap.add_argument("--hl", required=True)
    ap.add_argument("--dydx", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-json", default=None)
    ap.add_argument("--daily-snapshot", required=True)
    args = ap.parse_args(argv)

    base_dir = "."
    rows: List[Dict] = []
    rows += load_rows_or_placeholders(args.drift, base_dir, "drift", args.daily_snapshot)
    rows += load_rows_or_placeholders(args.hl, base_dir, "hyperliquid", args.daily_snapshot)
    rows += load_rows_or_placeholders(args.dydx, base_dir, "dydx", args.daily_snapshot)

    # deterministic sort
    rows.sort(key=lambda r: (str(r.get("exchange","")), str(r.get("symbol_raw",""))))

    # CSV
    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in CSV_FIELDS})

    # JSON (optional)
    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"[combine] rows={len(rows)} → {args.out_csv}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
