"""Classifies the Yahoo/IDX close_raw discrepancy population found by
jobs/reconcile.py's retroactive full-history scan into explicit,
testable categories — spec extension, Phase 2 follow-up investigation
(2026-08-08/09, materiality + AT_FLOOR fix 2026-08-11).

Two structurally different detection tracks, because the underlying
phenomena move differently:

Track 1 (categories A/B/C) — FROZEN-VALUE runs. A side is INERT in a
window if it isn't genuinely, materially trading there — either its
close never changes at all, or it changes but on too few real-volume
days to mean anything (see MATERIALITY below). If Yahoo is inert while
IDX shows real activity, that's Yahoo staleness (A if it coincides with
a market-wide cluster of other tickers doing the same thing at the same
time — a dateable event; B if it's isolated to this one ticker — a
Yahoo-side data gap with no external explanation). If BOTH sides are
inert, that's category C (folds into the zero-volume/suspension thread,
not a data-quality defect). If neither side is inert yet the run is
still discrepant, Track 1 has no label for it — that's Track 2's job.

MATERIALITY (added 2026-08-11): a side whose close value changes is not
automatically "moving" — CLAUDE.md's B-bucket audit found 10 tickers
(ASRM x2, FASW, WICO, BRNA, DSSA, PEGE, TFCO, LCKM, TRUS, CTBN) where
the "other side" ticked between 2-3 values on a handful of 100-1200
share trades scattered through an otherwise fully zero-volume run —
technically not bit-identical, but not real trading either. A side only
counts as showing real activity if its nonzero-volume-day fraction over
the run clears MATERIALITY_THRESHOLD (see sensitivity analysis in
HANDOFF.md before touching the default). Below threshold, that side is
inert regardless of whether its price technically moved. The mirror
case (FASW 86d, WIKA 73d, FISH 153d, frozen_side='idx' with Yahoo
"moving" on 1-2 real-volume days out of the whole run) is the same gap
from the other direction and reclassifies the same way.

AT_FLOOR (added 2026-08-11): Rp50 is IDX's minimum tick price ("gocap").
A ticker genuinely trading there — real, sometimes huge, volume, price
mechanically pinned because the exchange won't let it go lower — is
bit-identical close_raw across the run exactly like a real freeze is,
but it is NOT a defect: it's real market microstructure (see BNBR: 82%
of 2020-2023 days at exactly 50, up to 253M shares on a single day).
IDX is authoritative for close_raw (CLAUDE.md SOURCE AUTHORITY), so this
exception applies to IDX's side only — a Yahoo close that happens to
equal 50 is not evidence of anything, since Yahoo's close_raw isn't
trustworthy at the level this exception cares about. Concretely: IDX
literally-frozen-at-50-with-material-volume no longer counts as IDX
being inert, which flips 77 previously-category-C runs across 16
tickers (BBRM's 522-day run is the largest — IDX pinned at exactly 50
for 2+ years with real, sometimes million-share, volume while Yahoo sat
frozen at an unrelated 67.7745 the entire time) into category A/B —
they were "both sides stale, nothing to see" only because the detector
couldn't tell floor-pinning from staleness; IDX was fine the whole time
and Yahoo was the one silently wrong.

Track 2 (category D and "ambiguous") — RATIO-REGIME transitions. Unlike
a frozen run, a genuine corporate action has BOTH sources moving day to
day, just at a persistently different scale before vs. after a single
transition point — because one source (Yahoo) retroactively rescaled
its entire series and the other (IDX raw) didn't. Detected by splitting
each ticker's ratio series into stable regimes and checking the
transition between them for real matching volume on both sides — "real"
now means materiality-threshold volume across the transition window,
not just a nonzero day count on the single transition date (same fix as
Track 1, same threshold). A regime that later reverts to its
pre-transition ratio (TPMA-type) is explicitly NOT category D — it's
flagged "ambiguous" instead of forced into a label nobody checked.

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

# A side only counts as showing real activity in a window if at least this
# fraction of its days there carry nonzero volume. Below this, a technically
# non-identical close (a couple of 100-1200-share trades scattered through
# an otherwise-silent run) does not count as "the other side moved" — see
# module docstring's MATERIALITY section.
#
# 0.5 was chosen after a sensitivity sweep at 0.0/0.3/0.5/0.7 across every
# frozen run in the full ticker population (2026-08-11, see HANDOFF.md for
# the full table). Two findings support it, not just intuition:
#   1. Population-level effect is gradual, not a cliff: category B goes
#      108 -> 108 -> 105 -> 103 tickers and C goes 159 -> 163 -> 166 -> 174
#      across t=0.0/0.3/0.5/0.7 — a ~5% B/C shift end to end, no jump at
#      any single threshold.
#   2. The underlying distribution is why: of 656 frozen runs with a
#      non-inert "other side", 590 (90%) sit at exactly 1.0 volume-day
#      fraction (fully real trading, every day) and the remaining 66 are
#      thinly spread across 0.0-0.9 with no secondary cluster anywhere —
#      there is no natural gap to land a threshold on, so no choice in
#      [0.3, 0.7] is meaningfully more "correct" than another. 0.5 is the
#      midpoint of that flat region.
# All 10 originally-flagged tickers (ASRM, FASW, WICO, BRNA, DSSA, PEGE,
# TFCO, LCKM, TRUS, CTBN) plus the 3 mirror cases (FASW/WIKA/FISH, IDX
# literally-frozen with near-zero Yahoo volume) lose at least one run's B
# membership by t=0.5; a few (WICO, LCKM, CTBN) keep a *different*,
# genuinely-material run's B membership even past t=0.7 — expected, since
# not every divergence episode on a given ticker is thin, only some are.
# Configurable per-call, not just a constant, because this is a judgment
# call, not a physical law.
DEFAULT_MATERIALITY_THRESHOLD = 0.5

# IDX's minimum tick price ("gocap" in local trading slang) — a hard
# exchange-enforced floor, not a data artifact. See module docstring's
# AT_FLOOR section. Only ever applied to IDX's close_raw: IDX is the
# authoritative source for close_raw (CLAUDE.md SOURCE AUTHORITY), Yahoo's
# is not, so a Yahoo close that happens to equal 50 tells us nothing about
# real market microstructure and must not trigger this exception.
IDX_FLOOR_PRICE = 50.0


@dataclass
class FrozenRun:
    ticker: str
    start: dt.date
    end: dt.date
    n_days: int
    frozen_side: str        # 'yahoo' | 'idx' | 'both' — which side is INERT (see docstring), not merely bit-identical
    other_side_moved: bool  # did the non-inert side show real, materially-backed activity
    other_side_volume_frac: float | None = None  # nonzero-volume-day fraction of the non-inert side, for audit
    at_floor: bool = False  # True if this run's "inert" IDX side is actually genuine floor-pinned trading


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
    yahoo_vol_frac_at_transition: float = 1.0  # nonzero-volume-day fraction over the transition window
    idx_vol_frac_at_transition: float = 1.0


@dataclass
class ClassificationResult:
    category_a: set[str] = field(default_factory=set)
    category_b: set[str] = field(default_factory=set)
    category_c: set[str] = field(default_factory=set)
    category_d: set[str] = field(default_factory=set)
    ambiguous: set[str] = field(default_factory=set)
    unclassified: set[str] = field(default_factory=set)
    at_floor_tickers: set[str] = field(default_factory=set)  # informational: has >=1 AT_FLOOR run; not a partition of the above

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


def _volume_frac(window: list[tuple], vol_index: int) -> float:
    """Fraction of days in `window` with nonzero volume at `vol_index`
    (3 for yahoo_vol, 4 for idx_vol in the (date, yc, ic, yv, iv) tuple)."""
    if not window:
        return 0.0
    return sum(1 for e in window if e[vol_index]) / len(window)


def find_frozen_runs(
    ticker: str,
    entries: list[tuple],
    materiality_threshold: float = DEFAULT_MATERIALITY_THRESHOLD,
) -> list[FrozenRun]:
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
            # Bit-identical across the FULL window, not merely >= the
            # noise-floor threshold — a short coincidental repeat (e.g. two
            # days landing on the same price by chance) must not count as
            # frozen just because it happens to clear the same low bar the
            # real (long) run cleared. Bug found via ADMF: idx_run_len=2
            # (coincidence) at FROZEN_RUN_MIN_DAYS=2 was misread as "idx
            # also frozen", downgrading a clean 289-day yahoo-only freeze
            # to category C ("both frozen").
            yahoo_literal_frozen = yahoo_run_len == run_len
            idx_literal_frozen = idx_run_len == run_len
            any_discrepant = any(_is_discrepant(yc, ic) for _, yc, ic, _, _ in window)

            if any_discrepant:
                idx_moved = any(ic != ic0 for _, _, ic, _, _ in window)
                yahoo_moved = any(yc != yc0 for _, yc, _, _, _ in window)
                yahoo_vol_frac = _volume_frac(window, 3)
                idx_vol_frac = _volume_frac(window, 4)

                # AT_FLOOR: IDX literally pinned at the exchange floor with
                # materially real volume is not staleness — see module
                # docstring. Yahoo has no equivalent exception: it isn't
                # authoritative for close_raw, so a Yahoo close of 50 is
                # not evidence of anything.
                idx_at_floor = idx_literal_frozen and ic0 == IDX_FLOOR_PRICE and idx_vol_frac >= materiality_threshold

                # A side shows real activity if it's floor-pinned (idx
                # only), or if it changed value AND that change is backed
                # by real volume on enough of the run's days — see
                # MATERIALITY in the module docstring.
                idx_real_activity = idx_at_floor or (idx_moved and idx_vol_frac >= materiality_threshold)
                yahoo_real_activity = yahoo_moved and yahoo_vol_frac >= materiality_threshold
                idx_inert = not idx_real_activity
                yahoo_inert = not yahoo_real_activity

                if yahoo_inert and not idx_inert:
                    frozen_side, other_moved, other_frac = "yahoo", True, idx_vol_frac
                elif idx_inert and not yahoo_inert:
                    frozen_side, other_moved, other_frac = "idx", True, yahoo_vol_frac
                elif yahoo_inert and idx_inert:
                    frozen_side, other_moved, other_frac = "both", False, None
                else:
                    # Both sides show real, materially-backed activity yet
                    # the run is still discrepant throughout — Track 1 has
                    # no label for "genuinely both trading, persistently at
                    # different levels"; that's Track 2's (regime
                    # transition) territory, not a frozen-run shape.
                    frozen_side, other_moved, other_frac = None, False, None

                if frozen_side:
                    runs.append(
                        FrozenRun(
                            ticker=ticker,
                            start=window[0][0],
                            end=window[-1][0],
                            n_days=len(window),
                            frozen_side=frozen_side,
                            other_side_moved=other_moved,
                            other_side_volume_frac=other_frac,
                            at_floor=idx_at_floor,
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

        # Materiality (same fix as Track 1, module docstring): a single
        # nonzero-volume day right at the transition isn't proof the new
        # regime is real trading — check the volume-day fraction across the
        # whole post-regime, the same window whose ratio we just trusted.
        post_window = valid[s : e + 1]
        yahoo_vol_frac = _volume_frac(post_window, 3)
        idx_vol_frac = _volume_frac(post_window, 4)

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
                yahoo_vol_frac_at_transition=yahoo_vol_frac,
                idx_vol_frac_at_transition=idx_vol_frac,
                reverts_later=reverts_later,
            )
        )
    return transitions


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def classify(
    session,
    tickers: list[str],
    materiality_threshold: float = DEFAULT_MATERIALITY_THRESHOLD,
) -> ClassificationResult:
    by_ticker = fetch_paired_series(session, tickers)
    result = ClassificationResult()

    all_frozen_runs = []
    for ticker, entries in by_ticker.items():
        all_frozen_runs.extend(find_frozen_runs(ticker, entries, materiality_threshold))
    result.frozen_runs = all_frozen_runs

    clusters = find_market_wide_clusters(all_frozen_runs)
    result.clusters = clusters

    for run in all_frozen_runs:
        if run.at_floor:
            result.at_floor_tickers.add(run.ticker)
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
            has_real_volume = (
                t.yahoo_vol_at_transition > 0
                and t.idx_vol_at_transition > 0
                and t.yahoo_vol_frac_at_transition >= materiality_threshold
                and t.idx_vol_frac_at_transition >= materiality_threshold
            )
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
def main(
    materiality_threshold: float = typer.Option(
        DEFAULT_MATERIALITY_THRESHOLD,
        help="Minimum nonzero-volume-day fraction for a side to count as genuinely, materially trading.",
    ),
) -> None:
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
        result = classify(session, all_tickers, materiality_threshold)

    print(f"Materiality threshold: {materiality_threshold}")
    print(f"Category A (market-wide Yahoo staleness): {len(result.category_a)} tickers")
    print(f"Category B (isolated Yahoo data freeze):   {len(result.category_b)} tickers")
    print(f"Category C (stale quote, both sides):      {len(result.category_c)} tickers")
    print(f"Category D (genuine corporate action):     {len(result.category_d)} tickers")
    print(f"Ambiguous (reverts, not forced into D):     {len(result.ambiguous)} tickers")
    print(f"Unclassified:                               {len(result.unclassified)} tickers")
    print(f"AT_FLOOR (IDX genuinely pinned at Rp50):    {len(result.at_floor_tickers)} tickers")
    print(f"\nMarket-wide clusters found: {len(result.clusters)}")
    for c in result.clusters:
        print(f"  {c['start']} -> {c['end']}: {len(c['tickers'])} tickers")


if __name__ == "__main__":
    app()
