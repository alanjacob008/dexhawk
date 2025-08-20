# -*- coding: utf-8 -*-
"""
File IO utilities: atomic writes, symbol registry, history append with dedupe.
"""
import os, json, tempfile, shutil, csv
from typing import List, Dict, Tuple
from .schema import CSV_FIELDS

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def atomic_write_text(path: str, text: str):
    ensure_dir(os.path.dirname(path) or ".")
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=os.path.dirname(path) or ".")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)

def write_json(path: str, obj):
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2))

def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ---- symbol registry ----
def registry_path(base_dir: str, exchange: str) -> str:
    return os.path.join(base_dir, "symbol_registry", f"{exchange.lower()}_symbols.json")

def read_symbol_registry(base_dir: str, exchange: str) -> List[str]:
    path = registry_path(base_dir, exchange)
    if not os.path.exists(path):
        return []
    try:
        js = read_json(path)
        if isinstance(js, dict) and "symbols" in js:
            return [str(s).upper() for s in js["symbols"]]
        if isinstance(js, list):
            return [str(s).upper() for s in js]
        return []
    except Exception:
        return []

def write_symbol_registry(base_dir: str, exchange: str, symbols: List[str]):
    ensure_dir(os.path.join(base_dir, "symbol_registry"))
    unique = sorted({str(s).upper() for s in symbols})
    write_json(registry_path(base_dir, exchange), {"symbols": unique})

# ---- history append with dedupe on (date, exchange, symbol) ----
def append_history_rows(history_dir: str, snapshot_date: str, rows: List[Dict]):
    ensure_dir(history_dir)
    year = snapshot_date[:4]
    path = os.path.join(history_dir, f"metrics_{year}.csv")

    existing_keys = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                k = (r.get("daily_snapshot",""), r.get("exchange","").lower(), r.get("symbol_raw","").upper())
                existing_keys.add(k)

    # filter to non-duplicates
    to_write = []
    for r in rows:
        k = (r.get("daily_snapshot",""), r.get("exchange","").lower(), r.get("symbol_raw","").upper())
        if k not in existing_keys:
            to_write.append(r)

    mode = "a" if os.path.exists(path) else "w"
    with open(path, mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if mode == "w":
            w.writeheader()
        for r in to_write:
            w.writerow({k: r.get(k, "") for k in CSV_FIELDS})
