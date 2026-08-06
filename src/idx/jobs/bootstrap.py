"""One-time full backfill (spec §3.1).

Phase 0 scope: seed/securities_seed.csv currently holds one ticker (AMMN) so
the pipeline can be proven end-to-end before Phase 1 seeds the full IDX
listed-company list into that same file. Nothing here is ticker-count
specific except the trading_calendar derivation, which is meaningless below
a handful of tickers and is skipped (with a log line) until then.

Runnable locally per spec §8:
    python -m idx.jobs.bootstrap --tickers AMMN
"""
from __future__ import annotations

import csv
import datetime as dt
import time
from pathlib import Path

import structlog
import typer
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from idx.db.models import IngestRun, PriceDaily, Security
from idx.db.session import session_scope
from idx.sources.base import PriceSource
from idx.sources.yahoo import YahooSource

log = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SEED_PATH = REPO_ROOT / "seed" / "securities_seed.csv"
COLD_STORAGE_ROOT = REPO_ROOT / "data" / "cold" / "prices_daily"

BATCH_SIZE = 50
BATCH_PAUSE_SECONDS = 1.5
MIN_TICKERS_FOR_CALENDAR_DERIVATION = 5

app = typer.Typer(add_completion=False)


def load_seed_rows(seed_path: Path) -> list[dict]:
    with seed_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def upsert_securities(session, rows: list[dict]) -> None:
    """Insert or refresh reference data. Idempotent (spec §0 principle 5)."""
    for row in rows:
        stmt = pg_insert(Security).values(
            ticker=row["ticker"],
            yahoo_symbol=row["yahoo_symbol"],
            name=row.get("name") or None,
            sector=row.get("sector") or None,
            sub_industry=row.get("sub_industry") or None,
            listing_date=row.get("listing_date") or None,
            delisting_date=row.get("delisting_date") or None,
            board=row.get("board") or None,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker"],
            set_={
                "yahoo_symbol": stmt.excluded.yahoo_symbol,
                "name": stmt.excluded.name,
                "sector": stmt.excluded.sector,
                "sub_industry": stmt.excluded.sub_industry,
                "listing_date": stmt.excluded.listing_date,
                "delisting_date": stmt.excluded.delisting_date,
                "board": stmt.excluded.board,
            },
        )
        session.execute(stmt)


def backfill_prices_for_ticker(
    session, source: PriceSource, security: Security
) -> int:
    """Fetch full history for one ticker and upsert into prices_daily.

    Re-running bootstrap for the same ticker/date/source is idempotent:
    values are refreshed in place rather than duplicated (spec §0 principle
    5). This differs from jobs/daily.py, where a value change appends a new
    row with a fresh ingested_at to preserve revision history (spec §0
    principle 2) — bootstrap has no prior "as-of" observation to preserve.
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
        stmt = pg_insert(PriceDaily).values(**values)
        update_cols = {
            k: getattr(stmt.excluded, k)
            for k in (
                "open_raw",
                "high_raw",
                "low_raw",
                "close_raw",
                "close_adj",
                "volume",
                "value_traded",
                "frequency",
            )
        }
        update_cols["ingested_at"] = dt.datetime.now(dt.timezone.utc)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker", "date", "source"], set_=update_cols
        )
        session.execute(stmt)
        rows_written += 1

    return rows_written


def _none_if_nan(value):
    import pandas as pd

    if value is None or pd.isna(value):
        return None
    return value


def snapshot_to_parquet(session, tickers: list[str], run_date: dt.date) -> None:
    """Spec §3.1 step 6: snapshot to Parquet under data/cold/prices_daily/."""
    import pandas as pd

    rows = session.execute(
        select(PriceDaily).where(PriceDaily.ticker.in_(tickers))
    ).scalars()
    records = [
        {
            "ticker": r.ticker,
            "date": r.date,
            "source": r.source,
            "open_raw": r.open_raw,
            "high_raw": r.high_raw,
            "low_raw": r.low_raw,
            "close_raw": r.close_raw,
            "close_adj": r.close_adj,
            "volume": r.volume,
            "value_traded": r.value_traded,
            "frequency": r.frequency,
            "ingested_at": r.ingested_at,
        }
        for r in rows
    ]
    if not records:
        log.warning("no_rows_to_snapshot")
        return

    out_dir = COLD_STORAGE_ROOT / f"date={run_date.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame.from_records(records).to_parquet(
        out_dir / "prices_daily.parquet", index=False
    )
    log.info("parquet_snapshot_written", path=str(out_dir), rows=len(records))


def maybe_derive_trading_calendar(session, tickers: list[str]) -> None:
    """Spec §3.1 step 5. Skipped below MIN_TICKERS_FOR_CALENDAR_DERIVATION —
    the "≥30% of active tickers" rule isn't meaningful with only a couple of
    tickers seeded. Runs for real once Phase 1 seeds the full universe."""
    if len(tickers) < MIN_TICKERS_FOR_CALENDAR_DERIVATION:
        log.info(
            "trading_calendar_derivation_skipped",
            reason="too few tickers seeded",
            ticker_count=len(tickers),
            threshold=MIN_TICKERS_FOR_CALENDAR_DERIVATION,
        )
        return
    raise NotImplementedError(
        "trading_calendar derivation for full universe is Phase 1 scope"
    )


def _chunk(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


@app.command()
def main(
    tickers: str = typer.Option(
        None,
        help="Comma-separated tickers to restrict this run to (default: all rows in seed file).",
    ),
    seed_path: Path = typer.Option(DEFAULT_SEED_PATH, help="Path to securities seed CSV."),
    dry_run: bool = typer.Option(False, help="Fetch and log, but do not write to the database."),
) -> None:
    started_at = dt.datetime.now(dt.timezone.utc)
    seed_rows = load_seed_rows(seed_path)
    if tickers:
        wanted = {t.strip() for t in tickers.split(",")}
        seed_rows = [r for r in seed_rows if r["ticker"] in wanted]

    if not seed_rows:
        log.error("no_seed_rows_matched", tickers=tickers, seed_path=str(seed_path))
        raise typer.Exit(code=1)

    log.info("bootstrap_start", ticker_count=len(seed_rows), dry_run=dry_run)

    source = YahooSource()
    rows_written_total = 0
    tickers_failed = 0
    ticker_list = [r["ticker"] for r in seed_rows]

    with session_scope() as session:
        if not dry_run:
            upsert_securities(session, seed_rows)
            session.flush()

        for batch_num, batch in enumerate(_chunk(seed_rows, BATCH_SIZE)):
            for row in batch:
                security = session.get(Security, row["ticker"]) if not dry_run else Security(
                    ticker=row["ticker"], yahoo_symbol=row["yahoo_symbol"]
                )
                try:
                    if dry_run:
                        df = source.fetch_history(security.yahoo_symbol)
                        log.info("dry_run_fetch", ticker=security.ticker, rows=len(df))
                        rows_written_total += len(df)
                    else:
                        n = backfill_prices_for_ticker(session, source, security)
                        rows_written_total += n
                        log.info("ticker_backfilled", ticker=security.ticker, rows=n)
                except Exception as exc:  # noqa: BLE001 - one ticker failing must not fail the run
                    tickers_failed += 1
                    log.error("ticker_backfill_failed", ticker=row["ticker"], error=str(exc))

            if batch_num < len(_chunk(seed_rows, BATCH_SIZE)) - 1:
                time.sleep(BATCH_PAUSE_SECONDS)

        if not dry_run:
            maybe_derive_trading_calendar(session, ticker_list)
            snapshot_to_parquet(session, ticker_list, run_date=started_at.date())

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
                    tickers_attempted=len(seed_rows),
                    tickers_failed=tickers_failed,
                    error_summary=None if tickers_failed == 0 else f"{tickers_failed} ticker(s) failed",
                )
            )

    log.info(
        "bootstrap_done",
        rows_written=rows_written_total,
        tickers_attempted=len(seed_rows),
        tickers_failed=tickers_failed,
        status=status,
    )


if __name__ == "__main__":
    app()
