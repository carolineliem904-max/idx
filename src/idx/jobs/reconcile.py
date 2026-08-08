"""Cross-source reconciliation (Phase 2, spec extension).

Where Yahoo and IDX disagree on close_raw for the same (ticker, date)
beyond one tick, log it to price_discrepancies. Two distinct reasons this
matters, per instruction:
1. Permanent canary for the next 2007-style upstream defect (Phase 1a's
   finding: 118 OHLC violations concentrated on two specific dates,
   hitting many unrelated tickers at once — a real cross-source check
   would have caught that class of problem immediately).
2. Systematic per-ticker disagreement usually means an unhandled
   corporate action — exactly the leakage that ruins backtests quietly
   (spec §2.1: raw prices aren't split/dividend-adjusted; if Yahoo applies
   an adjustment IDX's raw feed doesn't know about, or vice versa, the two
   sources drift apart in a way that looks like "noise" but isn't).

"Beyond one tick" uses IDX's actual fraksi harga (tick size) schedule, not
an arbitrary tolerance — a real IDX price can only take values on that
grid, so two sources reporting adjacent tick values is not a discrepancy,
it's rounding. The schedule below is the current (2023+) one; spec §6
itself flags that these rules "have been revised multiple times", so this
is knowingly not exact for older dates — same honesty standard as
everywhere else in this codebase.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from idx.db.models import PriceDiscrepancy

log = structlog.get_logger()

# IDX fraksi harga, current (2023+) schedule. See module docstring caveat.
_TICK_SIZE_BANDS = [
    (200, 1),
    (500, 2),
    (2000, 5),
    (5000, 10),
    (float("inf"), 25),
]


def tick_size(price: float) -> float:
    for upper, tick in _TICK_SIZE_BANDS:
        if price < upper:
            return tick
    return _TICK_SIZE_BANDS[-1][1]


@dataclass
class Discrepancy:
    ticker: str
    date: dt.date
    yahoo_close: float
    idx_close: float
    diff_abs: float
    diff_ticks: float


def find_discrepancies(session, date_start: dt.date, date_end: dt.date) -> list[Discrepancy]:
    rows = session.execute(
        text(
            """
            SELECT y.ticker, y.date, y.close_raw AS yahoo_close, i.close_raw AS idx_close
            FROM prices_daily_latest y
            JOIN prices_daily_latest i ON i.ticker = y.ticker AND i.date = y.date AND i.source = 'idx'
            WHERE y.source = 'yahoo' AND y.date BETWEEN :start AND :end
              AND y.close_raw IS NOT NULL AND i.close_raw IS NOT NULL
            """
        ),
        {"start": date_start, "end": date_end},
    ).all()

    discrepancies = []
    for r in rows:
        diff_abs = abs(float(r.yahoo_close) - float(r.idx_close))
        reference_price = float(r.idx_close)  # IDX raw is the tick-grid authority
        tick = tick_size(reference_price)
        diff_ticks = diff_abs / tick if tick else 0
        if diff_ticks > 1:
            discrepancies.append(
                Discrepancy(
                    ticker=r.ticker,
                    date=r.date,
                    yahoo_close=float(r.yahoo_close),
                    idx_close=float(r.idx_close),
                    diff_abs=diff_abs,
                    diff_ticks=diff_ticks,
                )
            )
    return discrepancies


def persist_discrepancies(session, discrepancies: list[Discrepancy]) -> None:
    for d in discrepancies:
        stmt = pg_insert(PriceDiscrepancy).values(
            ticker=d.ticker,
            date=d.date,
            yahoo_close=d.yahoo_close,
            idx_close=d.idx_close,
            diff_abs=d.diff_abs,
            diff_ticks=d.diff_ticks,
            detected_at=dt.datetime.now(dt.timezone.utc),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker", "date"],
            set_={
                "yahoo_close": stmt.excluded.yahoo_close,
                "idx_close": stmt.excluded.idx_close,
                "diff_abs": stmt.excluded.diff_abs,
                "diff_ticks": stmt.excluded.diff_ticks,
                "detected_at": stmt.excluded.detected_at,
            },
        )
        session.execute(stmt)


def reconcile(session, date_start: dt.date, date_end: dt.date) -> list[Discrepancy]:
    """Find and persist discrepancies for a date range. Returns what it
    found, for the caller (jobs/daily.py) to fold into its own report."""
    discrepancies = find_discrepancies(session, date_start, date_end)
    if discrepancies:
        persist_discrepancies(session, discrepancies)
        log.warning(
            "price_discrepancies_found",
            count=len(discrepancies),
            tickers=sorted({d.ticker for d in discrepancies}),
        )
    return discrepancies
