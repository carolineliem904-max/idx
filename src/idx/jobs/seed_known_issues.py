"""Seeds known_issues with what Phase 1 already found, before real
alerting exists to cry wolf about them (spec extension, Phase 2).

Idempotent by explicit existence check, not a DB unique constraint —
known_issues has nullable `ticker`/`date_start`/`date_end`, and Postgres
treats NULL != NULL in unique constraints, so a naive ON CONFLICT would
silently let date_range rows (ticker IS NULL) duplicate on every rerun.

Runnable locally: python -m idx.jobs.seed_known_issues
"""
from __future__ import annotations

import datetime as dt

import structlog
import typer
from sqlalchemy import text

from idx.db.models import KnownIssue
from idx.db.session import session_scope

log = structlog.get_logger()
app = typer.Typer(add_completion=False)


def _exists(session, **criteria) -> bool:
    conditions = " AND ".join(
        f"{k} IS NULL" if v is None else f"{k} = :{k}" for k, v in criteria.items()
    )
    params = {k: v for k, v in criteria.items() if v is not None}
    return session.execute(
        text(f"SELECT 1 FROM known_issues WHERE {conditions} LIMIT 1"), params
    ).first() is not None


def seed_ohlc_2007_anomaly(session) -> int:
    """Phase 1a finding: 118 OHLC-sanity violations concentrated on
    2026-08-06's Phase 1 validation run, dated 2007-01-10 and 2007-02-02,
    hitting 9+ unrelated tickers each on the same two dates — reads as an
    upstream Yahoo feed defect on those specific dates, not random noise
    or a pipeline bug. Historical fact, won't change: review_by=None."""
    written = 0
    for bad_date in (dt.date(2007, 1, 10), dt.date(2007, 2, 2)):
        if _exists(
            session, scope="date_range", ticker=None,
            date_start=bad_date, date_end=bad_date, check_name="ohlc_sanity",
        ):
            continue
        session.add(
            KnownIssue(
                scope="date_range",
                ticker=None,
                date_start=bad_date,
                date_end=bad_date,
                check_name="ohlc_sanity",
                reason=(
                    f"Phase 1a finding (2026-08-06): {bad_date} shows close outside "
                    f"[low,high] for 9+ unrelated tickers simultaneously — reads as an "
                    f"upstream Yahoo feed defect specific to this date, not a real "
                    f"OHLC violation or a pipeline bug."
                ),
                review_by=None,
            )
        )
        written += 1
    return written


def seed_suspended_tickers(session) -> int:
    """Phase 1a finding: ~40 active tickers under extended IDX trading
    suspension where Yahoo has only a stale quote instead of real history
    (e.g. WSKT/Waskita Karya, mid debt restructuring). Re-derived from the
    live DB right now (not a hardcoded list) so the seeded rows always
    match exactly what jobs/validate.py::check_insufficient_history finds
    — a typed-out ticker list risks a transcription error a query can't."""
    candidates = session.execute(
        text(
            """
            SELECT s.ticker, count(pd.date) AS n_bars
            FROM securities s
            LEFT JOIN prices_daily_latest pd ON pd.ticker = s.ticker AND pd.source = 'yahoo'
            WHERE s.is_active
            GROUP BY s.ticker
            HAVING count(pd.date) <= 2
            ORDER BY s.ticker
            """
        )
    ).all()

    written = 0
    review_by = dt.date.today() + dt.timedelta(days=365)  # re-check yearly: suspensions do get lifted
    for row in candidates:
        if _exists(
            session, scope="ticker", ticker=row.ticker,
            date_start=None, date_end=None, check_name="insufficient_yahoo_history",
        ):
            continue
        session.add(
            KnownIssue(
                scope="ticker",
                ticker=row.ticker,
                date_start=None,
                date_end=None,
                check_name="insufficient_yahoo_history",
                reason=(
                    f"Phase 1a finding: {row.ticker} had only {row.n_bars} Yahoo bar(s) "
                    f"despite being marked active — consistent with extended IDX trading "
                    f"suspension. Not fixable from our side (Yahoo simply has no history "
                    f"for a suspended name). Re-review yearly in case trading resumes."
                ),
                review_by=review_by,
            )
        )
        written += 1
    return written


@app.command()
def main() -> None:
    with session_scope() as session:
        n1 = seed_ohlc_2007_anomaly(session)
        n2 = seed_suspended_tickers(session)
    log.info("known_issues_seeded", ohlc_2007_rows=n1, suspended_ticker_rows=n2)
    print(f"Seeded {n1} OHLC-anomaly row(s) and {n2} suspended-ticker row(s) into known_issues.")


if __name__ == "__main__":
    app()
