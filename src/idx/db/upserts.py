"""Shared upsert helpers. Used by every job that writes reference data or
prices, so the idempotency contract (spec §0 principle 5) lives in one
place instead of being reimplemented per job."""
from __future__ import annotations

import datetime as dt

from sqlalchemy.dialects.postgresql import insert as pg_insert

from idx.db.models import PriceDaily, Security

_SECURITY_UPSERT_COLS = (
    "yahoo_symbol",
    "name",
    "sector",
    "sub_industry",
    "listing_date",
    "delisting_date",
    "board",
    "is_active",
)

_PRICE_UPSERT_COLS = (
    "open_raw",
    "high_raw",
    "low_raw",
    "close_raw",
    "close_adj",
    "volume",
    "value_traded",
    "frequency",
)


def upsert_security(session, values: dict) -> None:
    """Insert or refresh one `securities` row, keyed on `ticker`.

    `values` must include `ticker`; any of `_SECURITY_UPSERT_COLS` present
    are written on both INSERT and (for a pre-existing row) UPDATE. Columns
    omitted from `values` are left alone on UPDATE — but NOT on INSERT:
    SQLAlchemy still applies the model's Python-side column defaults (e.g.
    `Security.is_active` defaults to True) to a fresh row regardless of
    what `values` contains. A caller that only knows `ticker` +
    `yahoo_symbol` for a ticker whose active status is genuinely unknown
    must pass `is_active` explicitly (or use `ensure_security_placeholder`
    below) — omitting it does NOT mean "leave undetermined" on insert, it
    means "silently True". Getting this wrong is exactly what broke
    jobs/harvest_universe_history.py's delisted-ticker diff on the first
    run: every ticker it ever touched an FK-satisfying placeholder for
    came out is_active=True by default, so the diff against the active
    universe was empty by construction.
    """
    stmt = pg_insert(Security).values(**values)
    update_cols = {c: getattr(stmt.excluded, c) for c in _SECURITY_UPSERT_COLS if c in values}
    if not update_cols:
        stmt = stmt.on_conflict_do_nothing(index_elements=["ticker"])
    else:
        stmt = stmt.on_conflict_do_update(index_elements=["ticker"], set_=update_cols)
    session.execute(stmt)


def ensure_security_placeholder(session, ticker: str) -> None:
    """Insert a bare-bones `securities` row ONLY if one doesn't already
    exist, purely to satisfy prices_daily's FK — never touches an existing
    row's columns, in particular never flips an existing `is_active` either
    way. A brand-new row gets `is_active=False`: appearing in a historical
    IDX summary says nothing about whether a ticker is active *today*, so
    it must not default to True (see the warning on `upsert_security`
    above). This is the only correct way to write a not-yet-known ticker
    without corrupting the delisted-ticker diff.
    """
    stmt = pg_insert(Security).values(
        ticker=ticker, yahoo_symbol=f"{ticker}.JK", is_active=False
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["ticker"])
    session.execute(stmt)


def upsert_price_bar(session, values: dict) -> None:
    """Insert or refresh one `prices_daily` row, keyed on (ticker, date, source).

    Refreshing in place (not appending) is correct for bootstrap/backfill
    jobs, which have no prior "as-of" observation to preserve. jobs/daily.py
    (spec §3.2) is the one place that must instead append a new row with a
    fresh ingested_at when a value changes, to preserve revision history
    (spec §0 principle 2).
    """
    stmt = pg_insert(PriceDaily).values(**values)
    update_cols = {c: getattr(stmt.excluded, c) for c in _PRICE_UPSERT_COLS if c in values}
    update_cols["ingested_at"] = dt.datetime.now(dt.timezone.utc)
    stmt = stmt.on_conflict_do_update(
        index_elements=["ticker", "date", "source"], set_=update_cols
    )
    session.execute(stmt)
