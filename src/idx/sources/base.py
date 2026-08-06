"""Price source abstraction.

`jobs/bootstrap.py` and `jobs/daily.py` depend only on this interface, never
on a specific provider. Adding `sources/idx_official.py` or a paid API later
(spec §3.2 "Fallback if Yahoo breaks") means writing a new `PriceSource`
subclass, not touching the jobs. Never hardcode `.JK` here or anywhere else
(spec §8) — callers pass the `Security` row, which carries `yahoo_symbol`.
"""
from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod

import pandas as pd

# Columns every PriceSource.fetch_history() implementation must return,
# indexed by `date`. Missing fields (e.g. value_traded/frequency from Yahoo)
# are populated as null columns, not omitted.
PRICE_BAR_COLUMNS = [
    "open_raw",
    "high_raw",
    "low_raw",
    "close_raw",
    "close_adj",
    "volume",
    "value_traded",
    "frequency",
]


class PriceSource(ABC):
    #: value written to prices_daily.source, e.g. 'yahoo' | 'idx' | 'sectors'
    name: str

    @abstractmethod
    def fetch_history(
        self,
        yahoo_symbol: str,
        start: dt.date | None = None,
        end: dt.date | None = None,
    ) -> pd.DataFrame:
        """Fetch daily bars for one security.

        `start=None` means "full available history" (used by bootstrap).
        Returns a DataFrame indexed by `date` (date, not datetime) with
        exactly `PRICE_BAR_COLUMNS`, one row per trading day in range.
        An empty DataFrame (same columns, zero rows) signals "no data
        available" rather than raising, so callers can log and continue.
        """
        raise NotImplementedError
