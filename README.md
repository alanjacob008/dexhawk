# DexHawk — Project Guide

> A lightweight pipeline + UI to compare **Drift**, **Hyperliquid**, and **dYdX** markets (pairs, leverage, 24h volume, OI), with daily snapshots and an hourly “latest” view.

---

## What this repo does

* **Collectors (Python)** pull venue data:
  * `src/drift_collect.py` (Cosmic API)
  * `src/hl_collect.py` (Hyperliquid API)
  * `src/dydx_collect.py` (dYdX Indexer)

* **Combiner** merges the three JSONs → one CSV (`all_latest.csv`)

* **Publisher** writes:
  * `data/latest/` — the current “latest” JSONs + combined CSV (updated hourly)
  * `data/daily_snapshots/` — per-day files (created once daily)
  * `data/history/metrics_YYYY.csv` — yearly append-only history

* **UI (`index.html`)**:
  * Grouped table by `symbol_raw`
  * Exchange chips (Drift / dYdX / Hyperliquid)
  * Filters (symbol, market type, presence: *Not on dYdX/Drift/Hyperliquid*)
  * Sortable columns (Leverage, Vol, OI, Vol/OI, “vs Baseline” ×)
  * Group click → **Volume & OI charts** (daily by default, optional rolling)
  * Change tracker (market type / leverage changes)


## How it runs

### GitHub Actions (2 workflows)

* **Daily** — runs at 00:02 UTC: creates daily snapshots + appends history
* **Hourly** — runs every hour at :05 UTC: refreshes only `data/latest/`

> We already added a `--mode latest` switch so the hourly job doesn’t touch archives.

---

## UI notes

* **Header**: “DexHawk — Last updated … (UTC)” uses the HTTP `Last-Modified` of `data/latest/all_latest.csv`.
* **Charts**: default is **daily**; the “Rolling” input applies **rolling sum** for Volume and **rolling mean** for OI (shown as a note below charts).
* **Presence filter**: *All*, *Not on dYdX*, *Not on Drift*, *Not on Hyperliquid*.
* **Baseline compare**: select an exchange; table shows **Vol vs Base** and **OI vs Base** in × (e.g., `10.35×`, `selected`, `—`).
