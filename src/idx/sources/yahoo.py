"""Yahoo Finance price source (spec §1, §3.1). Throttles aggressively —
callers are responsible for batching/pausing (spec §3.1 step 3)."""
from __future__ import annotations

import datetime as dt

import pandas as pd
import structlog
import yfinance as yf

from idx.sources.base import PRICE_BAR_COLUMNS, PriceSource

log = structlog.get_logger()

_YF_COLUMN_MAP = {
    "Open": "open_raw",
    "High": "high_raw",
    "Low": "low_raw",
    "Close": "close_raw",
    "Adj Close": "close_adj",
    "Volume": "volume",
}


class YahooSource(PriceSource):
    name = "yahoo"

    def fetch_history(
        self,
        yahoo_symbol: str,
        start: dt.date | None = None,
        end: dt.date | None = None,
    ) -> pd.DataFrame:
        ticker = yf.Ticker(yahoo_symbol)

        kwargs: dict = {"interval": "1d", "auto_adjust": False}
        if start is None:
            kwargs["period"] = "max"
        else:
            kwargs["start"] = start
            # yfinance's `end` is exclusive; spec wants inclusive ranges.
            kwargs["end"] = (end or dt.date.today()) + dt.timedelta(days=1)

        raw = ticker.history(**kwargs)

        if raw.empty:
            log.warning("yahoo_empty_history", yahoo_symbol=yahoo_symbol)
            return pd.DataFrame(columns=PRICE_BAR_COLUMNS).rename_axis("date")

        df = raw.rename(columns=_YF_COLUMN_MAP)[list(_YF_COLUMN_MAP.values())]
        df.index = df.index.date  # tz-aware datetime -> plain date
        df.index.name = "date"

        # value_traded, frequency: not available from Yahoo (spec §2.1 says
        # "if available"). Emit as null columns so downstream schema is stable.
        df["value_traded"] = pd.NA
        df["frequency"] = pd.NA

        # yfinance emits volume as float; DB column is bigint.
        df["volume"] = df["volume"].astype("Int64")

        return df[PRICE_BAR_COLUMNS]
