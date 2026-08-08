"""Shared upsert helpers. Used by every job that writes reference data or
prices, so the idempotency contract (spec §0 principle 5) lives in one
place instead of being reimplemented per job."""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

from sqlalchemy import text
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


# Decimal places matching each column's declared NUMERIC scale (spec §2.1).
# Incoming values are quantized to these before comparison so a value
# read back from Postgres (already rounded to this scale) and a freshly
# fetched one compare exactly, instead of drifting apart from float/Decimal
# representation noise and registering as a false "revision".
_PRICE_COLUMN_SCALES = {
    "open_raw": 4,
    "high_raw": 4,
    "low_raw": 4,
    "close_raw": 4,
    "close_adj": 6,
    "value_traded": 2,
}


def _quantize(value, column: str):
    if value is None:
        return None
    scale = _PRICE_COLUMN_SCALES.get(column)
    if scale is None:  # volume, frequency — plain integers, no rounding needed
        return value
    return Decimal(str(value)).quantize(Decimal(1).scaleb(-scale))


def upsert_price_bar(session, values: dict) -> str:
    """Insert a new prices_daily row ONLY if this is the first observation
    for (ticker, date, source), or the incoming values differ from the
    current latest version. Returns "new" | "revised" | "unchanged".

    This is the write path spec §3.2 describes: revisions are appended as
    new rows with a fresh ingested_at, never mutated in place (spec §0
    principle 2) — but must NEVER blindly append regardless of whether
    anything actually changed. jobs/daily.py re-fetches a rolling 7-day
    window every run, so most re-reads are identical to what's already
    stored; blindly appending would turn ~7 days x ~962 tickers x 2
    sources =~ 13k redundant rows PER RUN (~5M/year) that record no new
    information, and would make a revision-count spike in the daily report
    meaningless noise instead of a real signal.

    Used by every prices_daily writer — bootstrap and the historical
    harvester too, not just daily.py. Without this everywhere, re-running
    bootstrap.py or harvest_universe_history.py would ALSO now silently
    duplicate the whole table on every rerun: the widened PK (ticker,
    date, source, ingested_at) no longer rejects a same-key re-insert the
    way the old (ticker, date, source) PK did.

    NOT safe against a second writer touching the same (ticker, date,
    source) between the read and the write — acceptable because every job
    that writes prices_daily runs sequentially, never two at once against
    overlapping data.
    """
    current = session.execute(
        text(
            """
            SELECT open_raw, high_raw, low_raw, close_raw, close_adj,
                   volume, value_traded, frequency
            FROM prices_daily_latest
            WHERE ticker = :ticker AND date = :date AND source = :source
            """
        ),
        {"ticker": values["ticker"], "date": values["date"], "source": values["source"]},
    ).mappings().one_or_none()

    incoming_quantized = {c: _quantize(values.get(c), c) for c in _PRICE_UPSERT_COLS}

    if current is not None and all(
        current[c] == incoming_quantized[c] for c in _PRICE_UPSERT_COLS
    ):
        return "unchanged"

    insert_values = dict(values)
    insert_values["ingested_at"] = dt.datetime.now(dt.timezone.utc)
    session.execute(pg_insert(PriceDaily).values(**insert_values))
    return "new" if current is None else "revised"
