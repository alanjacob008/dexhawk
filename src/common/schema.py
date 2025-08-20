# -*- coding: utf-8 -*-
"""
Common schema helpers.
"""

# ---- Fixed column order for CSVs (UI + daily) ----
CSV_FIELDS = [
    "exchange",
    "market_type",
    "symbol_raw",
    "leverage_max",
    "price_usd",
    "volume_24h_usd",
    "open_interest_base",
    "open_interest_usd",
    "daily_snapshot",
]

def normalize_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    # force -USD suffix if missing
    if "-" not in s and s:
        return f"{s}-USD"
    return s

def norm_market_type(s: str) -> str:
    t = (s or "").strip().upper()
    if t.startswith("CROSS"):
        return "CROSS"
    if t.startswith("ISOLATED"):
        return "ISOLATED"
    return ""  # blank when unknown

def as_float_or_blank(x):
    try:
        if x is None or x == "":
            return ""
        return float(x)
    except Exception:
        return ""

def as_int_or_blank(x):
    try:
        if x is None or x == "":
            return ""
        return int(x)
    except Exception:
        try:
            v = float(x)
            return int(v)
        except Exception:
            return ""
