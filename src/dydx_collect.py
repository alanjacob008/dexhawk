# -*- coding: utf-8 -*-
"""
dYdX collector → schema rows list (uses OPEN INTEREST ONLY from `openInterest`).

Rules:
- open_interest_base  = float(m["openInterest"])             # base units
- open_interest_usd   = open_interest_base * oraclePrice
- volume_24h_usd      = float(m["volume24H"])
- leverage_max        = int(1 / initialMarginFraction) if > 0
- market_type         = normalized ("CROSS" | "ISOLATED")
- price_usd           = float(m["oraclePrice"])

If a required numeric field is missing/blank, we emit "" (empty string) to
stay consistent with your combiner/UI expectations.
"""

import argparse
import asyncio
import datetime as dt
from typing import List, Dict, Any

from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from .common.schema import normalize_symbol, norm_market_type


# ========= helpers =========
def fnum(x):
    """Safe float → float or None for missing/blank."""
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def emit_num_or_blank(x):
    """Convert float/None → float or '' for JSON output."""
    return x if x is not None else ""


# ========= transform one market =========
def to_row(ticker: str, m: Dict[str, Any], snapshot_date: str) -> Dict[str, Any]:
    sym   = normalize_symbol(ticker)

    # required numerics (as floats or None)
    price = fnum(m.get("oraclePrice"))
    vol24 = fnum(m.get("volume24H"))

    # IMPORTANT: use openInterest (base units). Do NOT use baseOpenInterest here.
    oi_base = fnum(m.get("openInterest"))
    oi_usd  = (oi_base or 0.0) * (price or 0.0) if (oi_base is not None and price is not None) else None

    # leverage from initialMarginFraction
    imf = fnum(m.get("initialMarginFraction"))
    lev = int(1.0 / imf) if (imf and imf > 0) else None

    return {
        "exchange": "dydx",
        "market_type": norm_market_type(m.get("marketType")),
        "symbol_raw": sym,
        "leverage_max": emit_num_or_blank(lev),
        "price_usd": emit_num_or_blank(price),
        "volume_24h_usd": emit_num_or_blank(vol24),
        "open_interest_base": emit_num_or_blank(oi_base),
        "open_interest_usd": emit_num_or_blank(oi_usd),
        "daily_snapshot": snapshot_date,
    }


# ========= fetch from indexer =========
async def collect(indexer_url: str) -> Dict[str, Dict[str, Any]]:
    idx = IndexerClient(indexer_url)
    js = await idx.markets.get_perpetual_markets()
    return js.get("markets", {}) if isinstance(js, dict) else {}


# ========= CLI / main =========
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="dYdX → schema rows (openInterest-based OI).")
    ap.add_argument("--out", required=True, help="Output JSON path")
    ap.add_argument("--daily-snapshot", default=dt.datetime.utcnow().date().isoformat(), help="YYYY-MM-DD (UTC)")
    ap.add_argument("--indexer", default="https://indexer.dydx.trade", help="Indexer REST base")
    ap.add_argument("--symbols-out", default=None, help="Update symbol_registry file path")
    args = ap.parse_args(argv)

    markets = asyncio.run(collect(args.indexer))
    rows: List[Dict[str, Any]] = [to_row(t, m, args.daily_snapshot) for t, m in markets.items()]

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
