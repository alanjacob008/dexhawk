# -*- coding: utf-8 -*-
"""
dYdX collector → schema rows list.
"""
import argparse, asyncio, datetime as dt, json
from typing import List, Dict, Any
from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from .common.schema import normalize_symbol, norm_market_type

# ---- transform one market ----
def to_row(ticker: str, m: Dict[str, Any], snapshot_date: str) -> Dict[str, Any]:
    sym = normalize_symbol(ticker)
    price = float(m.get("oraclePrice")) if m.get("oraclePrice") not in (None, "") else ""
    vol24 = float(m.get("volume24H")) if m.get("volume24H") not in (None, "") else ""
    base_oi = float(m.get("baseOpenInterest")) if m.get("baseOpenInterest") not in (None, "") else ""
    oi_usd = (base_oi * price) if (isinstance(base_oi, float) and isinstance(price, float)) else ""
    imf = m.get("initialMarginFraction")
    lev = int(1.0/float(imf)) if imf not in (None, "") and float(imf) > 0 else ""

    return {
        "exchange": "dydx",
        "market_type": norm_market_type(m.get("marketType")),
        "symbol_raw": sym,
        "leverage_max": lev,
        "price_usd": price,
        "volume_24h_usd": vol24,
        "open_interest_base": base_oi,
        "open_interest_usd": oi_usd,
        "daily_snapshot": snapshot_date,
    }

async def collect(indexer_url: str) -> Dict[str, Dict[str, Any]]:
    idx = IndexerClient(indexer_url)
    js = await idx.markets.get_perpetual_markets()
    return js.get("markets", {}) if isinstance(js, dict) else {}

def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--daily-snapshot", default=dt.datetime.utcnow().date().isoformat())
    ap.add_argument("--indexer", default="https://indexer.dydx.trade")
    ap.add_argument("--symbols-out", default=None)
    args = ap.parse_args(argv)

    markets = asyncio.run(collect(args.indexer))
    rows: List[Dict[str,Any]] = [to_row(t, m, args.daily_snapshot) for t, m in markets.items()]

    # write rows
    from .common.io_utils import write_json, write_symbol_registry
    write_json(args.out, rows)

    # registry update on success
    if args.symbols_out:
        write_symbol_registry(base_dir=".", exchange="dydx", symbols=[r["symbol_raw"] for r in rows])

    print(f"[dydx] rows={len(rows)} → {args.out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
