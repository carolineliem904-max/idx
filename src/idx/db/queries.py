"""Point-in-time-aware reads for prices_daily.

Two distinct query shapes, deliberately not conflated:
- `latest_price`: "what do we currently believe" — reads prices_daily_latest,
  the view spec §3.2 itself names ("the latest wins in views"). For
  dashboards, quick checks, anything that wants today's best-known value.
- `price_as_of`: "what did we know AT THE TIME" — spec principle #1's
  actual leakage guard. Filters on ingested_at <= as_of, not just picks the
  global latest. The Phase 4 feature builder must use this, not
  `latest_price`, for every table it reads — this module only covers
  prices_daily today, but the pattern (filter on created_at/ingested_at <=
  as_of, no exceptions) is meant to generalize to security_control and
  rumors too when the feature builder is built.
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy import text


def latest_price(session, ticker: str, date: dt.date, source: str):
    return session.execute(
        text(
            """
            SELECT * FROM prices_daily_latest
            WHERE ticker = :ticker AND date = :date AND source = :source
            """
        ),
        {"ticker": ticker, "date": date, "source": source},
    ).mappings().one_or_none()


def price_as_of(session, ticker: str, date: dt.date, source: str, as_of: dt.datetime):
    """The row that was true as of `as_of` — i.e. the latest version whose
    ingested_at does not exceed it. None if nothing had been ingested yet.
    This is the leakage guard: a naive "latest" read here would let a
    future correction bleed into a backtest that shouldn't be able to see
    it yet (spec principle #1)."""
    return session.execute(
        text(
            """
            SELECT * FROM prices_daily
            WHERE ticker = :ticker AND date = :date AND source = :source
              AND ingested_at <= :as_of
            ORDER BY ingested_at DESC
            LIMIT 1
            """
        ),
        {"ticker": ticker, "date": date, "source": source, "as_of": as_of},
    ).mappings().one_or_none()
