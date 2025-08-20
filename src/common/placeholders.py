# -*- coding: utf-8 -*-
"""
Placeholder rows when a venue fails entirely.
"""
from typing import List, Dict
from .schema import normalize_symbol

def make_placeholders(exchange: str, symbols: List[str], daily_snapshot: str) -> List[Dict]:
    rows = []
    for s in symbols:
        sym = normalize_symbol(s)
        rows.append({
            "exchange": exchange,
            "market_type": "",              # unknown
            "symbol_raw": sym,
            "leverage_max": "",             # unknown
            "price_usd": "",                # unknown
            "volume_24h_usd": 0.0,          # explicit zeros per policy
            "open_interest_base": 0.0,
            "open_interest_usd": 0.0,
            "daily_snapshot": daily_snapshot,
        })
    return rows
