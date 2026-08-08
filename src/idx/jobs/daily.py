"""jobs/daily.py — incremental daily update (spec §3.2).

Both sources every run, per explicit instruction: Yahoo (ticker-indexed,
one range fetch per ticker) AND sources/idx_official.py (date-indexed, one
fetch per calendar day covering every ticker) — different axes, so they're
fetched differently but land in the same prices_daily table with different
`source` values, upserted through the same idempotent, insert-only-on-
change path (db/upserts.py::upsert_price_bar) that Yahoo/IDX/bootstrap all
share.

The IDX side reuses jobs/harvest_universe_history.py::harvest_one_day
directly rather than reimplementing it — that function already is
"fetch one IDX day, resumable, respects the publish-lag grace window,
updates trading_calendar with ground truth". Phase 1b built it to walk a
historical range; here it walks today's rolling window instead. Same
function, same guarantees, no duplicated logic to drift out of sync.

Hooks for Part D (jobs/validate.py, inline) and Part E (cross-source
reconciliation) land in step 4 below once those exist — not yet in this
commit (see TODO markers).

Runnable locally per spec §8:
    python -m idx.jobs.daily --date 2026-08-04 --dry-run
"""
from __future__ import annotations

import datetime as dt
import time
from pathlib import Path

import structlog
import typer
from sqlalchemy import select, text

from idx.db.models import IngestRun, Security, TradingCalendar
from idx.db.session import session_scope
from idx.db.upserts import upsert_price_bar
from idx.jobs.harvest_universe_history import harvest_one_day
from idx.jobs.reconcile import reconcile
from idx.jobs.validate import print_report, run_validation
from idx.sources.yahoo import YahooSource

log = structlog.get_logger()
app = typer.Typer(add_completion=False)

REPO_ROOT = Path(__file__).resolve().parents[3]
COLD_STORAGE_ROOT = REPO_ROOT / "data" / "cold" / "prices_daily"

ROLLING_WINDOW_DAYS = 7  # spec §3.2 step 2 — catches Yahoo's late revisions/backfills
BATCH_SIZE = 50
BATCH_PAUSE_SECONDS = 1.5
RETRY_BACKOFF_SECONDS = (5, 20, 60)  # spec §3.2's stated retry policy


def _chunk(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _none_if_nan(value):
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    return value


def is_known_non_trading_day(session, date: dt.date) -> bool:
    """spec §3.2 step 1. Weekends are always known in advance (no DB hit
    needed). A weekday is only skippable if trading_calendar explicitly
    says so (a pre-annotated holiday, spec §3.1 step 5's "hand-review and
    annotate" step, or a day already confirmed non-trading by a previous
    run). If there's no row yet for `date` — the normal case for "today",
    which nothing has looked at yet — we do NOT skip; the run itself
    determines whether today was a trading day, via the IDX pass below.
    """
    if date.weekday() >= 5:
        return True
    row = session.get(TradingCalendar, date)
    return row is not None and not row.is_trading_day


def _window_dates(run_date: dt.date) -> list[dt.date]:
    start = run_date - dt.timedelta(days=ROLLING_WINDOW_DAYS - 1)
    return [start + dt.timedelta(days=i) for i in range(ROLLING_WINDOW_DAYS)]


def yahoo_pass(
    session, tickers: list[tuple[str, str]], window_start: dt.date, window_end: dt.date
) -> dict:
    """Fetch each active ticker's rolling window from Yahoo, upsert
    idempotently. Retries per spec §3.2: 3 attempts, backoff 5/20/60s.
    A ticker that still fails after retries increments `failed` and is
    simply retried again tomorrow by the rolling window — it does not
    fail the run (spec §3.2).

    `tickers` is a list of (ticker, yahoo_symbol) plain tuples, not ORM
    Security instances — those get detached the moment the session that
    loaded them closes (session_scope() closes on exit), and this runs in
    a different session than the one that loaded the ticker list.
    """
    source = YahooSource()
    stats = {"new": 0, "revised": 0, "unchanged": 0, "failed": 0}
    batches = _chunk(tickers, BATCH_SIZE)

    for batch_num, batch in enumerate(batches):
        for ticker, yahoo_symbol in batch:
            for attempt, backoff in enumerate((0, *RETRY_BACKOFF_SECONDS), start=1):
                if backoff:
                    time.sleep(backoff)
                try:
                    df = source.fetch_history(yahoo_symbol, start=window_start, end=window_end)
                    for date, bar in df.iterrows():
                        values = {
                            "ticker": ticker,
                            "date": date,
                            "source": "yahoo",
                            "open_raw": _none_if_nan(bar["open_raw"]),
                            "high_raw": _none_if_nan(bar["high_raw"]),
                            "low_raw": _none_if_nan(bar["low_raw"]),
                            "close_raw": _none_if_nan(bar["close_raw"]),
                            "close_adj": _none_if_nan(bar["close_adj"]),
                            "volume": _none_if_nan(bar["volume"]),
                            "value_traded": _none_if_nan(bar["value_traded"]),
                            "frequency": _none_if_nan(bar["frequency"]),
                        }
                        outcome = upsert_price_bar(session, values)
                        stats[outcome] += 1
                    break
                except Exception as exc:  # noqa: BLE001
                    if attempt <= len(RETRY_BACKOFF_SECONDS):
                        log.warning("yahoo_ticker_retry", ticker=ticker, attempt=attempt, error=str(exc))
                        continue
                    stats["failed"] += 1
                    log.error("yahoo_ticker_failed", ticker=ticker, error=str(exc))

        if batch_num < len(batches) - 1:
            time.sleep(BATCH_PAUSE_SECONDS)

    return stats


def idx_pass(window_dates: list[dt.date], known_tickers: set[str]) -> dict:
    """Fetch each window date from IDX via harvest_one_day — same function
    Phase 1b's historical harvest uses, so the publish-lag grace window,
    trading_calendar ground-truth updates, and idempotent upsert behavior
    are identical, not reimplemented."""
    stats = {"rows_written": 0, "trading_days": 0, "dates_failed": 0, "dates_undetermined": 0}
    for date in window_dates:
        if date.weekday() >= 5:
            continue
        try:
            outcome = harvest_one_day(date, known_tickers)
            if outcome.was_trading_day is None:
                stats["dates_undetermined"] += 1
                continue
            stats["trading_days"] += int(outcome.was_trading_day)
            stats["rows_written"] += outcome.rows_written
        except Exception as exc:  # noqa: BLE001 - one bad date must not fail the run
            stats["dates_failed"] += 1
            log.error("idx_date_failed", date=str(date), error=str(exc))
    return stats


def append_daily_slice_to_parquet(
    session, tickers: list[str], window_start: dt.date, window_end: dt.date, run_date: dt.date
) -> None:
    """spec §3.2 step 5: append the day's slice, not a full re-snapshot
    (that's bootstrap's job). Reads prices_daily_latest, scoped to just
    this run's rolling window."""
    import pandas as pd

    rows = session.execute(
        text(
            """
            SELECT ticker, date, source, open_raw, high_raw, low_raw,
                   close_raw, close_adj, volume, value_traded, frequency,
                   ingested_at
            FROM prices_daily_latest
            WHERE ticker = ANY(:tickers) AND date BETWEEN :start AND :end
            """
        ),
        {"tickers": tickers, "start": window_start, "end": window_end},
    ).mappings()
    records = [dict(r) for r in rows]
    if not records:
        log.warning("no_rows_to_snapshot_daily")
        return

    out_dir = COLD_STORAGE_ROOT / f"date={run_date.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame.from_records(records).to_parquet(out_dir / "daily_slice.parquet", index=False)
    log.info("daily_slice_parquet_written", path=str(out_dir), rows=len(records))


@app.command()
def main(
    date: dt.datetime = typer.Option(
        None, "--date", help="Override 'today' for testing (spec §8). Format YYYY-MM-DD."
    ),
    tickers: str = typer.Option(
        None, help="Comma-separated tickers to restrict this run to (default: all active). Testing/debugging convenience, not a production flag."
    ),
    dry_run: bool = typer.Option(False, help="Fetch and log, but do not write to the database."),
) -> None:
    run_date = date.date() if date else dt.date.today()
    wanted = {t.strip() for t in tickers.split(",")} if tickers else None
    started_at = dt.datetime.now(dt.timezone.utc)
    window_dates = _window_dates(run_date)
    window_start, window_end = window_dates[0], window_dates[-1]

    with session_scope() as session:
        if is_known_non_trading_day(session, run_date):
            log.info("daily_skipped_non_trading_day", date=str(run_date))
            if not dry_run:
                session.add(
                    IngestRun(
                        job_name="daily",
                        started_at=started_at,
                        finished_at=dt.datetime.now(dt.timezone.utc),
                        status="success",
                        rows_written=0,
                        tickers_attempted=0,
                        tickers_failed=0,
                        error_summary=f"skipped: {run_date} is a known non-trading day",
                    )
                )
            return

        # Plain tuples, not ORM Security instances — those detach the
        # moment this `with` block exits, and everything below runs in a
        # different session_scope().
        stmt = select(Security).where(Security.is_active.is_(True))
        if wanted:
            stmt = stmt.where(Security.ticker.in_(wanted))
        ticker_list = [(s.ticker, s.yahoo_symbol) for s in session.execute(stmt).scalars()]
        known_tickers = set(session.execute(select(Security.ticker)).scalars())

    log.info(
        "daily_start",
        run_date=str(run_date),
        window_start=str(window_start),
        window_end=str(window_end),
        tickers=len(ticker_list),
        dry_run=dry_run,
    )

    if dry_run:
        # Fetch-and-log only — mirrors bootstrap.py/seed_universe.py's
        # dry-run contract (spec §8): leaves zero trace in the database.
        source = YahooSource()
        yahoo_rows = 0
        for ticker, yahoo_symbol in ticker_list[:5]:  # sample, not the full universe — dry-run is a smoke test
            df = source.fetch_history(yahoo_symbol, start=window_start, end=window_end)
            yahoo_rows += len(df)
        log.info("dry_run_yahoo_sample", tickers_sampled=min(5, len(ticker_list)), rows=yahoo_rows)
        log.info("daily_done", dry_run=True)
        return

    with session_scope() as session:
        yahoo_stats = yahoo_pass(session, ticker_list, window_start, window_end)

    idx_stats = idx_pass(window_dates, known_tickers)

    # spec §3.2 step 4: run validate.py inline (in-process, not a
    # subprocess) over this run's own rolling window. A non-suppressed
    # failure marks the run "partial", same severity as a fetch failure —
    # a fetch that "succeeds" but writes bad data is not actually a
    # success.
    with session_scope() as session:
        validation_report = run_validation(session, window_start, window_end)
    if validation_report.failed:
        log.error(
            "daily_validation_failed",
            failures=len(validation_report.failures),
            suppressed=len(validation_report.suppressed),
        )
        print_report(validation_report)

    # Cross-source reconciliation: permanent canary for the next
    # 2007-style upstream defect, and systematic per-ticker disagreement
    # usually means an unhandled corporate action. Informational, not a
    # run-status failure by itself — Part F's alerting treats "new
    # discrepancies above a threshold" as its own trigger, separate from
    # fetch/validation failures, so it isn't folded into `status` here.
    with session_scope() as session:
        discrepancies = reconcile(session, window_start, window_end)

    rows_written_total = yahoo_stats["new"] + yahoo_stats["revised"] + idx_stats["rows_written"]
    tickers_failed = yahoo_stats["failed"] + idx_stats["dates_failed"]
    status = "success" if tickers_failed == 0 and not validation_report.failed else "partial"

    with session_scope() as session:
        append_daily_slice_to_parquet(
            session, [t[0] for t in ticker_list], window_start, window_end, run_date
        )
        error_parts = []
        if tickers_failed:
            error_parts.append(f"{tickers_failed} fetch failure(s)")
        if validation_report.failed:
            error_parts.append(f"{len(validation_report.failures)} validation failure(s)")
        if discrepancies:
            error_parts.append(f"{len(discrepancies)} price discrepancy(ies) (informational)")
        session.add(
            IngestRun(
                job_name="daily",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                status=status,
                rows_written=rows_written_total,
                tickers_attempted=len(ticker_list),
                tickers_failed=tickers_failed,
                error_summary="; ".join(error_parts) if error_parts else None,
            )
        )

    log.info(
        "daily_done",
        status=status,
        rows_written=rows_written_total,
        yahoo_new=yahoo_stats["new"],
        yahoo_revised=yahoo_stats["revised"],
        yahoo_unchanged=yahoo_stats["unchanged"],
        yahoo_failed=yahoo_stats["failed"],
        idx_rows_written=idx_stats["rows_written"],
        idx_trading_days=idx_stats["trading_days"],
        idx_dates_failed=idx_stats["dates_failed"],
        idx_dates_undetermined=idx_stats["dates_undetermined"],
        validation_failures=len(validation_report.failures),
        validation_suppressed=len(validation_report.suppressed),
        price_discrepancies=len(discrepancies),
    )


if __name__ == "__main__":
    app()
