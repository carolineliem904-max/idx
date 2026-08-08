"""Phase 1b — historical universe harvest via IDX's daily trading summary.

Two purposes, one pass over the data:
1. Discover the delisted tail. jobs/seed_universe.py only ever sees IDX's
   *current* listed-company directory (spec §0 principle 4 violation by
   construction). Any ticker that traded on IDX at some point but isn't in
   that directory today must be found some other way — here, by taking the
   union of every StockCode this endpoint has ever reported and diffing it
   against the active universe. `reconcile-delisted` does that diff.
2. Write real IDX raw prices (source='idx') alongside Yahoo's (source='yahoo')
   — an independent cross-check, and the only price history that exists at
   all for tickers Yahoo doesn't carry.

Hard limit, discovered empirically and NOT an artifact of this code: the
underlying endpoint has no data before 2020-01-02. The original ask was
"back to ~2010" — that goal is not reachable through this endpoint. Dates
before 2020-01-02 are out of scope here and the gap is not silently closed;
see README "Data completeness".

Resumable by construction: each trading day commits in its own transaction,
and a day already recorded in `trading_calendar` (by this job specifically
— tagged via the `note` prefix "idx:") is skipped on re-run without an API
call. Killing this process mid-run and restarting picks up within one day
of where it left off.

Runnable locally:
    python -m idx.jobs.harvest_universe_history harvest
    python -m idx.jobs.harvest_universe_history harvest --start 2024-01-01 --end 2024-01-31
    python -m idx.jobs.harvest_universe_history reconcile-delisted
    python -m idx.jobs.harvest_universe_history resync-calendar

`resync-calendar` exists because of a real race: running this harvester
concurrently with jobs/bootstrap.py (which also writes trading_calendar,
via a cruder ">=30% of tickers" heuristic over whatever date range Yahoo
happens to cover) let bootstrap's blanket upsert overwrite already-correct
IDX ground-truth rows for every date it had already covered at the moment
bootstrap ran — because bootstrap upserts unconditionally, with no
awareness that a better-sourced row might already exist. `resync-calendar`
re-derives trading_calendar for the harvested range purely from what's
already in prices_daily(source='idx') — no re-fetching — and is safe to
run any time drift like this is suspected.
"""
from __future__ import annotations

import datetime as dt
import time

import structlog
import typer
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from idx.db.models import IngestRun, PriceDaily, Security, TradingCalendar
from idx.db.session import session_scope
from idx.db.upserts import ensure_security_placeholder, upsert_price_bar, upsert_security
from idx.sources.idx_official import EARLIEST_AVAILABLE_DATE, fetch_stock_summary

log = structlog.get_logger()

app = typer.Typer(add_completion=False)

NOTE_PREFIX = "idx:"
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = (5, 20, 60)  # spec §3.2 daily-job retry policy, reused here
REQUEST_PAUSE_SECONDS = 1.2

# IDX's own GetStockSummary lags real trading activity by more than Yahoo
# does — verified empirically: querying "today" during/shortly after a live
# session returns recordsTotal=0 even though the market plainly traded
# (Yahoo already had a close). An empty response inside this window means
# "not published yet", not "non-trading day" — recording it as the latter
# would permanently poison that date via the skip-on-resume check below.
PUBLISH_LAG_GRACE_DAYS = 3


def _already_processed(session, date: dt.date) -> bool:
    row = session.get(TradingCalendar, date)
    return row is not None and (row.note or "").startswith(NOTE_PREFIX)


def _upsert_calendar_day(session, date: dt.date, is_trading_day: bool, note: str) -> None:
    stmt = pg_insert(TradingCalendar).values(date=date, is_trading_day=is_trading_day, note=note)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date"],
        set_={"is_trading_day": stmt.excluded.is_trading_day, "note": stmt.excluded.note},
    )
    session.execute(stmt)


def _fetch_with_retry(date: dt.date):
    last_exc = None
    for attempt, backoff in enumerate((0, *RETRY_BACKOFF_SECONDS), start=1):
        if backoff:
            time.sleep(backoff)
        try:
            return fetch_stock_summary(date)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.warning("harvest_fetch_retry", date=str(date), attempt=attempt, error=str(exc))
    raise last_exc  # type: ignore[misc]


class DayOutcome:
    def __init__(self, made_api_call: bool, was_trading_day: bool | None, rows_written: int):
        self.made_api_call = made_api_call
        self.was_trading_day = was_trading_day  # None = left undetermined (publish-lag window)
        self.rows_written = rows_written


def harvest_one_day(date: dt.date, known_tickers: set[str]) -> DayOutcome:
    """Commits its own transaction (crash-safe resumability, see module docstring).

    `known_tickers` is a live, caller-owned cache of tickers already present
    in `securities`, checked before every placeholder insert attempt so we
    only ever call ensure_security_placeholder for a ticker once, ever —
    not once per (ticker, day). Skipping this cache is what caused a
    989-row table to balloon to 84MB: Postgres's ON CONFLICT DO NOTHING
    still performs a speculative insertion internally, so ~1.6M redundant
    insert-then-abort attempts against an already-populated row left ~1.6M
    dead tuples behind — reclaimed once via VACUUM FULL, but the pattern
    would silently re-inflate the table on every future harvest run if left
    unfixed (regular autovacuum reclaims dead-tuple space for reuse, it
    does not shrink the file)."""
    with session_scope() as session:
        if _already_processed(session, date):
            row = session.get(TradingCalendar, date)
            return DayOutcome(made_api_call=False, was_trading_day=bool(row.is_trading_day), rows_written=0)

        df = _fetch_with_retry(date)
        if df.empty:
            if (dt.date.today() - date).days < PUBLISH_LAG_GRACE_DAYS:
                log.info("day_undetermined_publish_lag", date=str(date))
                return DayOutcome(made_api_call=True, was_trading_day=None, rows_written=0)
            _upsert_calendar_day(session, date, False, f"{NOTE_PREFIX} no data (holiday or gap)")
            return DayOutcome(made_api_call=True, was_trading_day=False, rows_written=0)

        rows_written = 0
        for ticker, bar in df.iterrows():
            values = {
                "ticker": ticker,
                "date": date,
                "source": "idx",
                "open_raw": _none_if_nan(bar["open_raw"]),
                "high_raw": _none_if_nan(bar["high_raw"]),
                "low_raw": _none_if_nan(bar["low_raw"]),
                "close_raw": _none_if_nan(bar["close_raw"]),
                "close_adj": None,
                "volume": _none_if_nan(bar["volume"]),
                "value_traded": _none_if_nan(bar["value_traded"]),
                "frequency": _none_if_nan(bar["frequency"]),
            }
            # `ticker` may not exist in `securities` yet (delisted names
            # discovered only here) — prices_daily.ticker FKs to securities,
            # so make sure a bare-bones row exists before the price upsert.
            # Must be the FK-only placeholder, not upsert_security: it must
            # never mark an unknown ticker is_active=True (see db/upserts.py).
            # Only actually hits the DB once per never-before-seen ticker —
            # see the bloat warning on this function's docstring.
            if ticker not in known_tickers:
                ensure_security_placeholder(session, ticker)
                known_tickers.add(ticker)
            upsert_price_bar(session, values)
            rows_written += 1

        _upsert_calendar_day(session, date, True, f"{NOTE_PREFIX} verified via GetStockSummary")
        return DayOutcome(made_api_call=True, was_trading_day=True, rows_written=rows_written)


def _none_if_nan(value):
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    return value


def _date_range(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


@app.command()
def harvest(
    start: dt.datetime = typer.Option(
        EARLIEST_AVAILABLE_DATE.isoformat(), help="First date to harvest (YYYY-MM-DD)."
    ),
    end: dt.datetime = typer.Option(None, help="Last date to harvest (YYYY-MM-DD), default today."),
) -> None:
    start_date = start.date()
    end_date = end.date() if end else dt.date.today()
    if start_date < EARLIEST_AVAILABLE_DATE:
        log.warning(
            "start_clamped_to_earliest_available",
            requested=str(start_date),
            clamped_to=str(EARLIEST_AVAILABLE_DATE),
            reason="GetStockSummary has no data before this date (empirically verified)",
        )
        start_date = EARLIEST_AVAILABLE_DATE

    started_at = dt.datetime.now(dt.timezone.utc)
    dates_attempted = 0
    dates_failed = 0
    trading_days = 0
    rows_written_total = 0

    with session_scope() as session:
        known_tickers = set(session.execute(select(Security.ticker)).scalars())
    log.info("harvest_start", start=str(start_date), end=str(end_date), known_tickers=len(known_tickers))

    for date in _date_range(start_date, end_date):
        if date.weekday() >= 5:  # Sat/Sun — IDX never trades, skip without an API call
            with session_scope() as session:
                if not _already_processed(session, date):
                    _upsert_calendar_day(session, date, False, f"{NOTE_PREFIX} weekend")
            continue

        dates_attempted += 1
        try:
            outcome = harvest_one_day(date, known_tickers)
            if outcome.made_api_call:
                time.sleep(REQUEST_PAUSE_SECONDS)
            if outcome.was_trading_day is None:
                continue  # inside publish-lag grace window; leave undetermined, don't mark failed
            trading_days += int(outcome.was_trading_day)
            rows_written_total += outcome.rows_written
            if outcome.rows_written:
                log.info("day_harvested", date=str(date), rows=outcome.rows_written)
        except Exception as exc:  # noqa: BLE001 - one bad date must not kill an hours-long run
            dates_failed += 1
            log.error("day_harvest_failed", date=str(date), error=str(exc))

    finished_at = dt.datetime.now(dt.timezone.utc)
    status = "success" if dates_failed == 0 else "partial"
    with session_scope() as session:
        session.add(
            IngestRun(
                job_name="harvest_universe_history",
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                rows_written=rows_written_total,
                tickers_attempted=dates_attempted,  # repurposed: "units attempted" = trading-day fetches
                tickers_failed=dates_failed,
                error_summary=None if dates_failed == 0 else f"{dates_failed} date(s) failed",
            )
        )

    log.info(
        "harvest_done",
        dates_attempted=dates_attempted,
        dates_failed=dates_failed,
        trading_days=trading_days,
        rows_written=rows_written_total,
        status=status,
    )


@app.command("reconcile-delisted")
def reconcile_delisted() -> None:
    """Diff the union of tickers ever seen via IDX summaries against the
    active universe (jobs/seed_universe.py). Anything in the union but not
    currently active gets upserted as is_active=False, with delisting_date
    set to the LAST date it was observed trading — an inferred proxy, not
    an official IDX delisting-announcement date (spec §3.1 step 1 mentions
    "locally saved historical listing/delisting announcements" as the ideal
    source; we don't have those, so this is the best available substitute
    and is documented as such, not presented as authoritative).
    """
    started_at = dt.datetime.now(dt.timezone.utc)
    with session_scope() as session:
        idx_tickers = set(
            session.execute(
                select(PriceDaily.ticker).where(PriceDaily.source == "idx").distinct()
            ).scalars()
        )
        active_tickers = set(
            session.execute(
                select(Security.ticker).where(Security.is_active.is_(True))
            ).scalars()
        )
        candidates = idx_tickers - active_tickers
        log.info(
            "reconcile_delisted_start",
            idx_tickers_seen=len(idx_tickers),
            active_tickers=len(active_tickers),
            delisted_candidates=len(candidates),
        )

        for ticker in sorted(candidates):
            last_seen = session.execute(
                select(PriceDaily.date)
                .where(PriceDaily.ticker == ticker, PriceDaily.source == "idx")
                .order_by(PriceDaily.date.desc())
                .limit(1)
            ).scalar_one()
            upsert_security(
                session,
                {
                    "ticker": ticker,
                    "yahoo_symbol": f"{ticker}.JK",
                    "is_active": False,
                    "delisting_date": last_seen,
                },
            )

        session.add(
            IngestRun(
                job_name="reconcile_delisted",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                status="success",
                rows_written=len(candidates),
                tickers_attempted=len(candidates),
                tickers_failed=0,
                error_summary=None,
            )
        )

    log.warning(
        "reconcile_delisted_done",
        delisted_found=len(candidates),
        note="delisting_date is last-observed-trading, inferred from IDX summaries — "
        "not an official delisting announcement date. Coverage is bounded by "
        "EARLIEST_AVAILABLE_DATE (2020-01-02): tickers delisted before that "
        "are still invisible. See README.",
    )


@app.command("resync-calendar")
def resync_calendar() -> None:
    """Re-derive trading_calendar for [EARLIEST_AVAILABLE_DATE, max harvested
    date] from already-persisted prices_daily(source='idx') rows — ground
    truth by presence, no threshold, no re-fetching. Overwrites unconditionally
    for this range, so it heals the bootstrap-vs-harvest race described in
    the module docstring regardless of which job last touched a given date.
    """
    with session_scope() as session:
        idx_dates = set(
            session.execute(
                select(PriceDaily.date).where(PriceDaily.source == "idx").distinct()
            ).scalars()
        )
        if not idx_dates:
            log.warning("resync_calendar_skipped_no_idx_data")
            return

        span_end = max(idx_dates)
        written = 0
        for date in _date_range(EARLIEST_AVAILABLE_DATE, span_end):
            if date.weekday() >= 5:
                _upsert_calendar_day(session, date, False, f"{NOTE_PREFIX} weekend")
            elif date in idx_dates:
                _upsert_calendar_day(session, date, True, f"{NOTE_PREFIX} verified via GetStockSummary")
            else:
                _upsert_calendar_day(session, date, False, f"{NOTE_PREFIX} no data (holiday or gap)")
            written += 1

    log.info(
        "resync_calendar_done",
        days_written=written,
        span_start=str(EARLIEST_AVAILABLE_DATE),
        span_end=str(span_end),
        trading_days=len(idx_dates),
    )


if __name__ == "__main__":
    app()
