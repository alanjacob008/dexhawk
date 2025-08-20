# -*- coding: utf-8 -*-
"""
Drift (Cosmic API) collector → schema rows list.
"""
import argparse, datetime as dt
from typing import List, Dict, Any
from .common.net import get_json
from .common.schema import normalize_symbol

COSMIC_URL = "https://api.cosmic.markets/api/drift/markets?page={page}&size=200&sortField=marketIndex&sortOrder=ascend&status=all&minutes=1440"

def status_map(code) -> str:
    try:
        c = int(code)
        return "ACTIVE" if c == 1 else "INACTIVE"
    except Exception:
        return "ACTIVE"

def leverage_from_record(rec: Dict[str, Any]) -> int | str:
    # rule: if highLeverageInitialMarginRatioDecimal == 0 → use marginRatioInitialMultiplier else highLeverageInitialMarginMultiplier
    h_imr = rec.get("highLeverageInitialMarginRatioDecimal")
    if h_imr in (None, "", 0, 0.0):
        v = rec.get("marginRatioInitialMultiplier")
    else:
        v = rec.get("highLeverageInitialMarginMultiplier")
    try:
        return int(v)
    except Exception:
        return ""

def to_row(rec: Dict[str, Any], snapshot_date: str) -> Dict[str, Any]:
    base = str(rec.get("baseAssetSymbol","")).upper()
    sym = normalize_symbol(f"{base}-USD")
    price = rec.get("lastOraclePrice")
    price = float(price) if price not in (None,"") else ""
    vol24 = rec.get("recentVolume")
    vol24 = float(vol24) if vol24 not in (None,"") else ""
    oi_base = rec.get("openInterest")
    oi_base = float(oi_base) if oi_base not in (None,"") else ""
    oi_usd = (oi_base * price) if (isinstance(oi_base,float) and isinstance(price,float)) else ""

    return {
        "exchange": "drift",
        "market_type": "CROSS",
        "symbol_raw": sym,
        "leverage_max": leverage_from_record(rec),
        "price_usd": price,
        "volume_24h_usd": vol24,
        "open_interest_base": oi_base,
        "open_interest_usd": oi_usd,
        "daily_snapshot": snapshot_date,
        # status available but not needed in CSV (kept out by design)
    }

def fetch_all() -> List[Dict[str, Any]]:
    out = []
    page = 1
    while True:
        js = get_json(COSMIC_URL.format(page=page))
        content = js.get("content", []) if isinstance(js, dict) else []
        out.extend(content)
        total_pages = int(js.get("totalPages", 1))
        if page >= total_pages:
            break
        page += 1
    return out

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--daily-snapshot", default=dt.datetime.utcnow().date().isoformat())
    ap.add_argument("--symbols-out", default=None)
    args = ap.parse_args(argv)

    recs = fetch_all()
    rows = [to_row(r, args.daily_snapshot) for r in recs]

    from .common.io_utils import write_json, write_symbol_registry
    write_json(args.out, rows)
    if args.symbols_out:
        write_symbol_registry(base_dir=".", exchange="drift", symbols=[r["symbol_raw"] for r in rows])
    print(f"[drift] rows={len(rows)} → {args.out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
