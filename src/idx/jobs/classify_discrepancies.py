"""Classifies the Yahoo/IDX close_raw discrepancy population found by
jobs/reconcile.py's retroactive full-history scan into explicit,
testable categories — spec extension, Phase 2 follow-up investigation
(2026-08-08/09).

Two structurally different detection tracks, because the underlying
phenomena move differently:

Track 1 (categories A/B/C) — FROZEN-VALUE runs. One source's close_raw
is bit-identical across many consecutive days. If Yahoo is frozen while
IDX keeps moving with real volume, that's Yahoo staleness (A if it
coincides with a market-wide cluster of other tickers doing the same
thing at the same time — a dateable event; B if it's isolated to this
one ticker — a Yahoo-side data gap with no external explanation). If
BOTH sides are frozen/zero-volume, that's category C (folds into the
zero-volume/suspension thread, not a data-quality defect).

Track 2 (category D and "ambiguous") — RATIO-REGIME transitions. Unlike
a frozen run, a genuine corporate action has BOTH sources moving day to
day, just at a persistently different scale before vs. after a single
transition point — because one source (Yahoo) retroactively rescaled
its entire series and the other (IDX raw) didn't. Detected by splitting
each ticker's ratio series into stable regimes and checking the
transition between them for real matching volume on both sides. A
regime that later reverts to its pre-transition ratio (TPMA-type) is
explicitly NOT category D — it's flagged "ambiguous" instead of forced
into a label nobody checked.

Unclassified is a valid, expected outcome for anything that fits
neither track cleanly.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass, field

import structlog
import typer
from sqlalchemy import text

from idx.db.session import session_scope
from idx.jobs.reconcile import tick_size

log = structlog.get_logger()
app = typer.Typer(add_completion=False)

RATIO_TRANSITION_THRESHOLD = 0.005   # >0.5% ratio jump between adjacent rows = new regime
MIN_REGIME_DAYS = 5                  # ignore regimes shorter than this for Track 2 (noise floor)
MARKET_WIDE_CLUSTER_MIN_TICKERS = 20  # spec: "any cluster where >20 tickers diverge in the same window"
FROZEN_RUN_MIN_DAYS = 2              # minimum run length to even consider for Track 1


@dataclass
class FrozenRun:
    ticker: str
    start: dt.date
    end: dt.date
    n_days: int
    frozen_side: str        # 'yahoo' | 'idx' | 'both'
    other_side_moved: bool  # did the non-frozen side show real price movement


@dataclass
class RegimeTransition:
    ticker: str
    transition_date: dt.date
    pre_ratio: float
    post_ratio: float
    pre_regime_days: int
    post_regime_days: int
    yahoo_vol_at_transition: int
    idx_vol_at_transition: int
    reverts_later: bool


@dataclass
class ClassificationResult:
    category_a: set[str] = field(default_factory=set)
    category_b: set[str] = field(default_factory=set)
    category_c: set[str] = field(default_factory=set)
    category_d: set[str] = field(default_factory=set)
    ambiguous: set[str] = field(default_factory=set)
    unclassified: set[str] = field(default_factory=set)

    frozen_runs: list[FrozenRun] = field(default_factory=list)
    d_transitions: list[RegimeTransition] = field(default_factory=list)
    ambiguous_transitions: list[RegimeTransition] = field(default_factory=list)
    clusters: list[dict] = field(default_factory=list)


def fetch_paired_series(session, tickers: list[str]) -> dict[str, list[tuple]]:
    rows = session.execute(
        text(
            """
            SELECT y.ticker, y.date, y.close_raw AS yahoo_close, y.volume AS yahoo_vol,
                   i.close_raw AS idx_close, i.volume AS idx_vol
            FROM prices_daily_latest y
            JOIN prices_daily_latest i ON i.ticker = y.ticker AND i.date = y.date AND i.source = 'idx'
            WHERE y.source = 'yahoo' AND y.ticker = ANY(:tickers)
              AND y.close_raw IS NOT NULL AND i.close_raw IS NOT NULL
            ORDER BY y.ticker, y.date
            """
        ),
        {"tickers": tickers},
    ).all()
    by_ticker = defaultdict(list)
    for r in rows:
        by_ticker[r.ticker].append(
            (r.date, float(r.yahoo_close), float(r.idx_close), r.yahoo_vol or 0, r.idx_vol or 0)
        )
    return by_ticker


def _is_discrepant(yc: float, ic: float) -> bool:
    if ic is None or ic <= 1e-6:
        return False
    return abs(yc - ic) / tick_size(ic) > 1


# --------------------------------------------------------------------------
# Track 1: frozen-value runs (A / B / C)
# --------------------------------------------------------------------------


def find_frozen_runs(ticker: str, entries: list[tuple]) -> list[FrozenRun]:
    runs = []
    i = 0
    n = len(entries)
    while i < n:
        j = i
        yc0 = entries[i][1]
        ic0 = entries[i][2]
        while j + 1 < n and entries[j + 1][1] == yc0:
            j += 1
        yahoo_run_len = j - i + 1

        j2 = i
        while j2 + 1 < n and entries[j2 + 1][2] == ic0:
            j2 += 1
        idx_run_len = j2 - i + 1

        run_len = max(yahoo_run_len, idx_run_len)
        if run_len >= FROZEN_RUN_MIN_DAYS:
            window = entries[i : i + run_len]
            # "Frozen" means constant across the FULL window, not merely
            # >= the noise-floor threshold — a short coincidental repeat
            # (e.g. two days landing on the same price by chance) must not
            # count as frozen just because it happens to clear the same
            # low bar the real (long) run cleared. Bug found via ADMF:
            # idx_run_len=2 (coincidence) at FROZEN_RUN_MIN_DAYS=2 was
            # misread as "idx also frozen", downgrading a clean 289-day
            # yahoo-only freeze to category C ("both frozen").
            yahoo_frozen = yahoo_run_len == run_len
            idx_frozen = idx_run_len == run_len
            any_discrepant = any(_is_discrepant(yc, ic) for _, yc, ic, _, _ in window)

            if any_discrepant:
                idx_moved = any(ic != ic0 for _, _, ic, _, _ in window)
                yahoo_moved = any(yc != yc0 for _, yc, _, _, _ in window)
                if yahoo_frozen and idx_moved and not idx_frozen:
                    frozen_side, other_moved = "yahoo", True
                elif idx_frozen and yahoo_moved and not yahoo_frozen:
                    frozen_side, other_moved = "idx", True
                elif yahoo_frozen and idx_frozen:
                    frozen_side, other_moved = "both", False
                else:
                    frozen_side, other_moved = None, False

                if frozen_side:
                    runs.append(
                        FrozenRun(
                            ticker=ticker,
                            start=window[0][0],
                            end=window[-1][0],
                            n_days=len(window),
                            frozen_side=frozen_side,
                            other_side_moved=other_moved,
                        )
                    )
            i += run_len
        else:
            i += 1
    return runs


def find_market_wide_clusters(frozen_runs: list[FrozenRun]) -> list[dict]:
    """Any date where >= MARKET_WIDE_CLUSTER_MIN_TICKERS distinct tickers
    have an active yahoo-frozen (or idx-frozen) run is part of a cluster.
    Contiguous such dates are merged into one cluster window."""
    yahoo_stale = [r for r in frozen_runs if r.frozen_side == "yahoo"]
    per_date_tickers: dict[dt.date, set[str]] = defaultdict(set)
    for r in yahoo_stale:
        d = r.start
        while d <= r.end:
            per_date_tickers[d].add(r.ticker)
            d += dt.timedelta(days=1)

    hot_dates = sorted(d for d, tickers in per_date_tickers.items() if len(tickers) >= MARKET_WIDE_CLUSTER_MIN_TICKERS)
    if not hot_dates:
        return []

    clusters = []
    window_start = hot_dates[0]
    prev = hot_dates[0]
    window_tickers: set[str] = set(per_date_tickers[hot_dates[0]])
    for d in hot_dates[1:]:
        if (d - prev).days <= 5:  # merge nearby hot dates into one cluster window
            window_tickers |= per_date_tickers[d]
            prev = d
        else:
            clusters.append({"start": window_start, "end": prev, "tickers": window_tickers})
            window_start = d
            prev = d
            window_tickers = set(per_date_tickers[d])
    clusters.append({"start": window_start, "end": prev, "tickers": window_tickers})
    return clusters


def _run_overlaps_cluster(run: FrozenRun, clusters: list[dict]) -> bool:
    for c in clusters:
        if run.start <= c["end"] and run.end >= c["start"]:
            return True
    return False


# --------------------------------------------------------------------------
# Track 2: ratio-regime transitions (D / ambiguous)
# --------------------------------------------------------------------------


def find_regime_transitions(ticker: str, entries: list[tuple]) -> list[RegimeTransition]:
    """Splits the ratio series into stable regimes (>=MIN_REGIME_DAYS,
    ratio changes <RATIO_TRANSITION_THRESHOLD within a regime), then
    evaluates each transition between discrepant regimes."""
    valid = [(d, yc, ic, yv, iv) for d, yc, ic, yv, iv in entries if ic and ic > 1e-6]
    if len(valid) < MIN_REGIME_DAYS * 2:
        return []

    regimes = []  # list of (start_idx, end_idx, median_ratio)
    start = 0
    ratios = [yc / ic for _, yc, ic, _, _ in valid]
    for i in range(1, len(valid)):
        if abs(ratios[i] - ratios[i - 1]) > RATIO_TRANSITION_THRESHOLD:
            regimes.append((start, i - 1))
            start = i
    regimes.append((start, len(valid) - 1))

    # merge regimes shorter than MIN_REGIME_DAYS into the previous one (noise)
    merged = []
    for s, e in regimes:
        if merged and (e - s + 1) < MIN_REGIME_DAYS:
            ps, pe = merged[-1]
            merged[-1] = (ps, e)
        else:
            merged.append((s, e))

    def median_ratio(s, e):
        seg = sorted(ratios[s : e + 1])
        return seg[len(seg) // 2]

    transitions = []
    for k in range(1, len(merged)):
        ps, pe = merged[k - 1]
        s, e = merged[k]
        pre_ratio = median_ratio(ps, pe)
        post_ratio = median_ratio(s, e)
        if abs(post_ratio - 1.0) < 0.01 and abs(pre_ratio - 1.0) < 0.01:
            continue  # both sides already agree, not a real transition of interest
        if abs(post_ratio - pre_ratio) < RATIO_TRANSITION_THRESHOLD:
            continue

        transition_date, y_vol, i_vol = valid[s][0], valid[s][3], valid[s][4]
        reverts_later = False
        for later_k in range(k + 1, len(merged)):
            ls, le = merged[later_k]
            if abs(median_ratio(ls, le) - pre_ratio) < RATIO_TRANSITION_THRESHOLD:
                reverts_later = True
                break

        transitions.append(
            RegimeTransition(
                ticker=ticker,
                transition_date=transition_date,
                pre_ratio=round(pre_ratio, 4),
                post_ratio=round(post_ratio, 4),
                pre_regime_days=pe - ps + 1,
                post_regime_days=e - s + 1,
                yahoo_vol_at_transition=y_vol,
                idx_vol_at_transition=i_vol,
                reverts_later=reverts_later,
            )
        )
    return transitions


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def classify(session, tickers: list[str]) -> ClassificationResult:
    by_ticker = fetch_paired_series(session, tickers)
    result = ClassificationResult()

    all_frozen_runs = []
    for ticker, entries in by_ticker.items():
        all_frozen_runs.extend(find_frozen_runs(ticker, entries))
    result.frozen_runs = all_frozen_runs

    clusters = find_market_wide_clusters(all_frozen_runs)
    result.clusters = clusters

    for run in all_frozen_runs:
        if run.frozen_side == "both":
            result.category_c.add(run.ticker)
        elif run.frozen_side == "yahoo":
            if _run_overlaps_cluster(run, clusters):
                result.category_a.add(run.ticker)
            else:
                result.category_b.add(run.ticker)
        elif run.frozen_side == "idx":
            # Symmetric case: IDX frozen, Yahoo moving. Rare (IDX null-outs
            # zero-trade days already, source-level) but checked for
            # completeness, reported under B (isolated) since a market-wide
            # cluster of IDX-side freezes is not the hypothesis in play here.
            result.category_b.add(run.ticker)

    for ticker, entries in by_ticker.items():
        transitions = find_regime_transitions(ticker, entries)
        for t in transitions:
            has_real_volume = t.yahoo_vol_at_transition > 0 and t.idx_vol_at_transition > 0
            if has_real_volume and not t.reverts_later:
                result.category_d.add(ticker)
                result.d_transitions.append(t)
            elif has_real_volume and t.reverts_later:
                result.ambiguous.add(ticker)
                result.ambiguous_transitions.append(t)

    classified = result.category_a | result.category_b | result.category_c | result.category_d | result.ambiguous
    result.unclassified = set(tickers) - classified
    return result


@app.command()
def main() -> None:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT DISTINCT y.ticker
                FROM prices_daily_latest y
                JOIN prices_daily_latest i ON i.ticker = y.ticker AND i.date = y.date AND i.source = 'idx'
                WHERE y.source = 'yahoo' AND y.close_raw IS NOT NULL AND i.close_raw IS NOT NULL
                """
            )
        ).all()
        all_tickers = [r.ticker for r in rows]
        result = classify(session, all_tickers)

    print(f"Category A (market-wide Yahoo staleness): {len(result.category_a)} tickers")
    print(f"Category B (isolated Yahoo data freeze):   {len(result.category_b)} tickers")
    print(f"Category C (stale quote, both sides):      {len(result.category_c)} tickers")
    print(f"Category D (genuine corporate action):     {len(result.category_d)} tickers")
    print(f"Ambiguous (reverts, not forced into D):     {len(result.ambiguous)} tickers")
    print(f"Unclassified:                               {len(result.unclassified)} tickers")
    print(f"\nMarket-wide clusters found: {len(result.clusters)}")
    for c in result.clusters:
        print(f"  {c['start']} -> {c['end']}: {len(c['tickers'])} tickers")


if __name__ == "__main__":
    app()
