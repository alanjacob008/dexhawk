# -*- coding: utf-8 -*-
"""
Hyperliquid collector via /info metaAndAssetCtxs → schema rows.
"""
import argparse, datetime as dt
from typing import List, Dict, Any, Tuple
from .common.net import post_json
from .common.schema import normalize_symbol

INFO_URL = "https://api.hyperliquid.xyz/info"

def fetch_meta_asset_ctxs():
    payload = {"type": "metaAndAssetCtxs"}
    return post_json(INFO_URL, payload)

def parse_universe(js) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
    """
    Returns (universe, ctxs) where universe[i] maps to ctxs[i].
    """
    # HL returns [ { universe: [...] }, [ ctx, ctx, ... ] ]
    if isinstance(js, list) and len(js) >= 2 and isinstance(js[0], dict) and "universe" in js[0]:
        uni = js[0].get("universe") or []
        ctxs = js[1] if isinstance(js[1], list) else []
        return uni, ctxs
    # fallback: try dict form
    uni = js.get("universe", []) if isinstance(js, dict) else []
    ctxs = js.get("assetCtxs", []) if isinstance(js, dict) else []
    return uni, ctxs

def to_row(u: Dict[str,Any], c: Dict[str,Any], snapshot_date: str) -> Dict[str,Any]:
    name = str(u.get("name","")).upper()
    sym = normalize_symbol(f"{name}-USD")
    # price: oraclePx preferred
    price = c.get("oraclePx") or c.get("markPx") or c.get("midPx")
    price = float(price) if price not in (None,"") else ""
    vol24 = c.get("dayNtlVlm")
    vol24 = float(vol24) if vol24 not in (None,"") else ""
    oi_base = c.get("openInterest")
    oi_base = float(oi_base) if oi_base not in (None,"") else ""
    oi_usd = (oi_base * price) if (isinstance(oi_base,float) and isinstance(price,float)) else ""
    lev = u.get("maxLeverage")
    lev = int(lev) if lev not in (None,"") else ""

    return {
        "exchange": "hyperliquid",
        "market_type": "CROSS",
        "symbol_raw": sym,
        "leverage_max": lev,
        "price_usd": price,
        "volume_24h_usd": vol24,
        "open_interest_base": oi_base,
        "open_interest_usd": oi_usd,
        "daily_snapshot": snapshot_date,
    }

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--daily-snapshot", default=dt.datetime.utcnow().date().isoformat())
    ap.add_argument("--symbols-out", default=None)
    args = ap.parse_args(argv)

    js = fetch_meta_asset_ctxs()
    uni, ctxs = parse_universe(js)
    rows: List[Dict[str,Any]] = []
    for i, u in enumerate(uni):
        c = ctxs[i] if i < len(ctxs) else {}
        rows.append(to_row(u, c, args.daily_snapshot))

    from .common.io_utils import write_json, write_symbol_registry
    write_json(args.out, rows)
    if args.symbols_out:
        write_symbol_registry(base_dir=".", exchange="hyperliquid", symbols=[r["symbol_raw"] for r in rows])

    print(f"[hyperliquid] rows={len(rows)} → {args.out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
