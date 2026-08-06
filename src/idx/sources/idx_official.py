"""IDX official daily trading summary ("Ringkasan Saham") — TradingSummary
GetStockSummary endpoint.

Two jobs depend on this, not just one:
1. jobs/harvest_universe_history.py — discovers delisted tickers (spec §0
   principle 4) by taking the union of every StockCode ever seen here and
   diffing it against the active-only universe from sources/idx_company_list.py.
2. A future PriceSource fallback (spec §3.2 "Fallback if Yahoo breaks") —
   this already returns raw OHLCV per ticker per day, source='idx', which
   cross-checks Yahoo independently. Not wired up as a PriceSource yet
   (that's real Phase 2 scope), but the data it writes to prices_daily today
   is exactly what that fallback would consume.

IMPORTANT, discovered empirically (not documented anywhere public we could
find): this endpoint only serves data from 2020-01-02 onward. Every date
probed in 2010-2019 returns recordsTotal=0 with a 200 OK — it isn't rate
limiting or a wrong date format, the data simply isn't there. This means
harvesting this endpoint cannot, by itself, reach the "~2010" delisted-tail
goal — see README "Data completeness" for the honest current state.

No close_adj is emitted — IDX's raw feed carries no split-adjustment,
that's Yahoo's job (spec §2.1 "store raw and adjusted side by side").
"""
from __future__ import annotations

import datetime as dt

import pandas as pd
import structlog
from curl_cffi import requests

log = structlog.get_logger()

BASE_URL = "https://www.idx.co.id/primary/TradingSummary/GetStockSummary"
REFERER = "https://www.idx.co.id/en/market-data/trading-summary/stock-summary"
REQUEST_TIMEOUT_SECONDS = 30

EARLIEST_AVAILABLE_DATE = dt.date(2020, 1, 2)  # empirically verified lower bound

_COLUMN_MAP = {
    "OpenPrice": "open_raw",
    "High": "high_raw",
    "Low": "low_raw",
    "Close": "close_raw",
    "Volume": "volume",
    "Value": "value_traded",
    "Frequency": "frequency",
}


def fetch_stock_summary(date: dt.date) -> pd.DataFrame:
    """Fetch every ticker's OHLCV for one calendar day.

    Returns a DataFrame indexed by `ticker` with columns open_raw, high_raw,
    low_raw, close_raw, close_adj (always null), volume, value_traded,
    frequency, delisting_date_hint (IDX's own `DelistingDate` field, empty
    string when not applicable — observed always empty on same-day queries;
    kept in case it populates historically, but treat as a hint, not a
    source of truth). Empty DataFrame (zero rows) means no trading that day
    — a weekend, a holiday, or a date before EARLIEST_AVAILABLE_DATE.
    """
    resp = requests.get(
        BASE_URL,
        params={"date": date.strftime("%Y%m%d"), "start": 0, "length": 9999},
        impersonate="chrome",
        headers={"Referer": REFERER, "accept": "application/json, text/plain, */*"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    payload = resp.json()
    rows = payload.get("data", [])
    if not rows:
        return pd.DataFrame(
            columns=list(_COLUMN_MAP.values()) + ["close_adj", "delisting_date_hint"]
        ).rename_axis("ticker")

    df = pd.DataFrame(rows)
    df = df.rename(columns=_COLUMN_MAP)
    df["ticker"] = df["StockCode"]
    df["close_adj"] = pd.NA
    df["delisting_date_hint"] = df.get("DelistingDate", "")

    # IDX reports OpenPrice/High/Low as literal 0 (not null) on zero-trade
    # days, while Close carries the last-known price forward. Left as-is,
    # that trips "close outside [low, high]" in validate.py for every
    # untraded ticker on every date — a false positive, not a real
    # OHLC violation. Volume=0 is the real signal here (spec §3.3's own
    # zero-volume check depends on it), so we keep volume/value/frequency
    # as reported and null out open/high/low instead of the close.
    no_trade = df["volume"] == 0
    df.loc[no_trade, ["open_raw", "high_raw", "low_raw"]] = pd.NA

    df = df.set_index("ticker")
    return df[list(_COLUMN_MAP.values()) + ["close_adj", "delisting_date_hint"]]
