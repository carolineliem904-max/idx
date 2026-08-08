"""One-time full backfill (spec §3.1).

Ticker source: `securities` table (populated by jobs/seed_universe.py —
Phase 1a's active-only universe, survivorship-biased until
jobs/harvest_universe_history.py's reconcile-delisted step has also run;
see README "Data completeness"). `--seed-csv` remains as a dev/override
path for upserting a handful of rows before securities is fully seeded
(this is how Phase 0 backfilled AMMN alone).

Runnable locally per spec §8:
    python -m idx.jobs.bootstrap                    # all active tickers
    python -m idx.jobs.bootstrap --tickers AMMN      # just one
    python -m idx.jobs.bootstrap --dry-run
"""
from __future__ import annotations

import csv
import datetime as dt
import time
from pathlib import Path

import structlog
import typer
from sqlalchemy import select, text

from idx.db.models import IngestRun, PriceDaily, Security
from idx.db.session import session_scope
from idx.db.upserts import upsert_price_bar, upsert_security
from idx.sources.base import PriceSource
from idx.sources.yahoo import YahooSource

log = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parents[3]
COLD_STORAGE_ROOT = REPO_ROOT / "data" / "cold" / "prices_daily"
CHECKPOINT_PATH = REPO_ROOT / "data" / ".checkpoints" / "bootstrap_completed_tickers.txt"

BATCH_SIZE = 50
BATCH_PAUSE_SECONDS = 1.5
TICKER_RETRY_ATTEMPTS = 2
TICKER_RETRY_PAUSE_SECONDS = 3.0
TRADING_CALENDAR_THRESHOLD = 0.30  # spec §3.1 step 5: ">=30% of active tickers"

app = typer.Typer(add_completion=False)


def load_seed_rows(seed_path: Path) -> list[dict]:
    with seed_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_tickers_from_db(
    session, tickers_filter: set[str] | None, active_only: bool
) -> list[Security]:
    stmt = select(Security).order_by(Security.ticker)
    if active_only:
        stmt = stmt.where(Security.is_active.is_(True))
    if tickers_filter:
        stmt = stmt.where(Security.ticker.in_(tickers_filter))
    return list(session.execute(stmt).scalars())


def load_checkpoint() -> set[str]:
    if not CHECKPOINT_PATH.exists():
        return set()
    return {line.strip() for line in CHECKPOINT_PATH.read_text().splitlines() if line.strip()}


def append_checkpoint(ticker: str) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CHECKPOINT_PATH.open("a") as f:
        f.write(ticker + "\n")
        f.flush()


def backfill_prices_for_ticker(session, source: PriceSource, security: Security) -> int:
    """Fetch full history for one ticker and upsert into prices_daily.

    Re-running bootstrap for the same ticker/date/source is idempotent:
    upsert_price_bar only ever writes a new row when the value is actually
    new or changed (spec §0 principle 5), so a resumed/repeated bootstrap
    run doesn't duplicate rows under the widened (ticker, date, source,
    ingested_at) PK the way a blind append would.
    """
    df = source.fetch_history(security.yahoo_symbol, start=None, end=None)
    if df.empty:
        log.warning("no_history", ticker=security.ticker)
        return 0

    rows_written = 0
    for date, bar in df.iterrows():
        values = {
            "ticker": security.ticker,
            "date": date,
            "source": source.name,
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
        if outcome != "unchanged":
            rows_written += 1

    return rows_written


def _none_if_nan(value):
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    return value


def snapshot_to_parquet(session, tickers: list[str], run_date: dt.date) -> None:
    """Spec §3.1 step 6: snapshot to Parquet under data/cold/prices_daily/.

    Reads prices_daily_latest, not the base table — a feature-building
    snapshot should reflect the current best-known value per (ticker,
    date, source), not every revision ever recorded (spec §3.2 "the
    latest wins in views").
    """
    import pandas as pd

    rows = session.execute(
        text(
            """
            SELECT ticker, date, source, open_raw, high_raw, low_raw,
                   close_raw, close_adj, volume, value_traded, frequency,
                   ingested_at
            FROM prices_daily_latest
            WHERE ticker = ANY(:tickers)
            """
        ),
        {"tickers": tickers},
    ).mappings()
    records = [dict(r) for r in rows]
    if not records:
        log.warning("no_rows_to_snapshot")
        return

    out_dir = COLD_STORAGE_ROOT / f"date={run_date.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame.from_records(records).to_parquet(
        out_dir / "prices_daily.parquet", index=False
    )
    log.info("parquet_snapshot_written", path=str(out_dir), rows=len(records))


def derive_trading_calendar(session) -> int:
    """Spec §3.1 step 5: trading_calendar as the union of dates where >=30%
    of tickers-with-any-history have a non-null close, over the full span
    covered by prices_daily(source='yahoo').

    This is a heuristic, not ground truth — it can't tell a genuine market
    holiday from "most of the universe just hadn't listed yet" at the very
    start of the span, and it can't see the delisted tickers Phase 1a never
    seeded. jobs/harvest_universe_history.py later overwrites 2020-01-02+
    with real IDX-observed trading days (a row either exists in the IDX
    daily summary or it doesn't — no threshold needed), tagged with a
    different `note` so the two provenances stay distinguishable until
    that overwrite happens. Pre-2020 stays on this heuristic; the spec's
    "then hand-review and annotate holidays" step is still Caroline's, not
    automated here.

    Reads prices_daily_latest, not the base table: with revision history
    now possible (widened PK), a naive read of the base table would let a
    ticker with 2 recorded versions of the same day count twice toward
    that day's ">=30% of tickers" threshold.
    """
    result = session.execute(
        text(
            "SELECT date, ticker, close_raw FROM prices_daily_latest WHERE source = 'yahoo'"
        )
    ).all()
    if not result:
        log.warning("trading_calendar_skipped_no_price_data")
        return 0

    import pandas as pd

    df = pd.DataFrame(result, columns=["date", "ticker", "close_raw"])
    universe_size = df["ticker"].nunique()
    daily_close_counts = df[df["close_raw"].notna()].groupby("date")["ticker"].nunique()

    full_range = pd.date_range(df["date"].min(), df["date"].max(), freq="D").date
    written = 0
    from idx.db.models import TradingCalendar
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    for d in full_range:
        n_close = int(daily_close_counts.get(d, 0))
        is_trading_day = (n_close / universe_size) >= TRADING_CALENDAR_THRESHOLD
        stmt = pg_insert(TradingCalendar).values(
            date=d, is_trading_day=is_trading_day, note="heuristic: >=30% tickers (bootstrap)"
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["date"],
            set_={"is_trading_day": stmt.excluded.is_trading_day, "note": stmt.excluded.note},
        )
        session.execute(stmt)
        written += 1

    log.info(
        "trading_calendar_derived",
        days_written=written,
        universe_size=universe_size,
        span_start=str(full_range[0]),
        span_end=str(full_range[-1]),
    )
    return written


def _chunk(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


@app.command()
def main(
    tickers: str = typer.Option(
        None,
        help="Comma-separated tickers to restrict this run to (default: all active securities in DB).",
    ),
    seed_csv: Path = typer.Option(
        None,
        help="Optional CSV to upsert into securities before backfilling (dev/override path).",
    ),
    include_inactive: bool = typer.Option(
        False, help="Also backfill tickers marked is_active=False."
    ),
    force: bool = typer.Option(
        False, help="Ignore the resume checkpoint and refetch full history for every ticker."
    ),
    skip_calendar: bool = typer.Option(
        False, help="Skip trading_calendar derivation (useful for quick single-ticker runs)."
    ),
    dry_run: bool = typer.Option(False, help="Fetch and log, but do not write to the database."),
) -> None:
    started_at = dt.datetime.now(dt.timezone.utc)
    wanted = {t.strip() for t in tickers.split(",")} if tickers else None

    source = YahooSource()
    rows_written_total = 0
    tickers_failed = 0

    with session_scope() as session:
        if seed_csv is not None and not dry_run:
            for row in load_seed_rows(seed_csv):
                upsert_security(session, row)
            session.flush()

        securities = load_tickers_from_db(session, wanted, active_only=not include_inactive)
        if not securities:
            log.error("no_securities_matched", tickers=tickers, hint="run jobs/seed_universe.py first")
            raise typer.Exit(code=1)

        checkpoint = load_checkpoint() if (not force and not dry_run) else set()
        pending = [s for s in securities if s.ticker not in checkpoint]
        skipped = len(securities) - len(pending)

        log.info(
            "bootstrap_start",
            ticker_count=len(securities),
            pending=len(pending),
            skipped_via_checkpoint=skipped,
            dry_run=dry_run,
        )

        batches = _chunk(pending, BATCH_SIZE)
        for batch_num, batch in enumerate(batches):
            for security in batch:
                for attempt in range(1, TICKER_RETRY_ATTEMPTS + 1):
                    try:
                        if dry_run:
                            df = source.fetch_history(security.yahoo_symbol)
                            log.info("dry_run_fetch", ticker=security.ticker, rows=len(df))
                            rows_written_total += len(df)
                        else:
                            n = backfill_prices_for_ticker(session, source, security)
                            rows_written_total += n
                            append_checkpoint(security.ticker)
                            log.info("ticker_backfilled", ticker=security.ticker, rows=n)
                        break
                    except Exception as exc:  # noqa: BLE001 - one ticker failing must not fail the run
                        if attempt < TICKER_RETRY_ATTEMPTS:
                            log.warning(
                                "ticker_backfill_retry",
                                ticker=security.ticker,
                                attempt=attempt,
                                error=str(exc),
                            )
                            time.sleep(TICKER_RETRY_PAUSE_SECONDS)
                        else:
                            tickers_failed += 1
                            log.error(
                                "ticker_backfill_failed", ticker=security.ticker, error=str(exc)
                            )

            if batch_num < len(batches) - 1:
                time.sleep(BATCH_PAUSE_SECONDS)

        if not dry_run:
            all_tickers = [s.ticker for s in securities]
            if not skip_calendar:
                derive_trading_calendar(session)
            snapshot_to_parquet(session, all_tickers, run_date=started_at.date())

        finished_at = dt.datetime.now(dt.timezone.utc)
        status = "success" if tickers_failed == 0 else "partial"
        if not dry_run:
            # A dry run must leave zero trace in the database (spec §8:
            # every job runnable locally with --dry-run) — ingest_runs is
            # part of that database, not exempt from it.
            session.add(
                IngestRun(
                    job_name="bootstrap",
                    started_at=started_at,
                    finished_at=finished_at,
                    status=status,
                    rows_written=rows_written_total,
                    tickers_attempted=len(pending),
                    tickers_failed=tickers_failed,
                    error_summary=None if tickers_failed == 0 else f"{tickers_failed} ticker(s) failed",
                )
            )

    log.info(
        "bootstrap_done",
        rows_written=rows_written_total,
        tickers_attempted=len(pending),
        tickers_failed=tickers_failed,
        status=status,
    )


if __name__ == "__main__":
    app()
