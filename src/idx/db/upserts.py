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
    are written, columns omitted are left at the DB default (so a caller
    that only knows `ticker` + `yahoo_symbol` doesn't clobber `sector` etc.
    written by a previous, better-informed caller).
    """
    stmt = pg_insert(Security).values(**values)
    update_cols = {c: getattr(stmt.excluded, c) for c in _SECURITY_UPSERT_COLS if c in values}
    if not update_cols:
        stmt = stmt.on_conflict_do_nothing(index_elements=["ticker"])
    else:
        stmt = stmt.on_conflict_do_update(index_elements=["ticker"], set_=update_cols)
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
