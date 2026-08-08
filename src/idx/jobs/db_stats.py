"""Sizing diagnostics — run BEFORE any deployment/capacity decision.

Reports what's actually in Postgres today (measured, not estimated):
total DB size, per-table data vs index size (index bloat is often half the
total and the cheapest thing to fix), row counts, and per-index breakdown
for the largest table. Then projects 1yr/3yr forward under two scenarios:

  (i)  current tables only (organic daily accretion)
  (ii) (i) + Phase 3's broker_flow_daily, which is an order of magnitude
       bigger than everything we hold today (~960 tickers x ~100 broker
       codes x ~250 trading days =~ 24M rows/year) — broker_flow_daily
       doesn't exist yet, so its per-row footprint is an ANALYTICAL
       ESTIMATE (proxied from prices_daily's measured bytes/row, which has
       a similar column count/shape), clearly labeled as such and never
       conflated with the measured numbers.

Also reports what dropping pre-2020 Yahoo history from the production set
would save, since spec's leakage/embargo design (§6) already treats
2020-01-02+ as the honest modeling window (Phase 1b's coverage bound) —
older Yahoo history is exploration-only, not something the daily job or a
production deployment needs to carry.

Runnable locally: python -m idx.jobs.db_stats
"""
from __future__ import annotations

import datetime as dt

import structlog
import typer
from sqlalchemy import text

from idx.db.session import get_engine

log = structlog.get_logger()
app = typer.Typer(add_completion=False)

TRADING_DAYS_PER_YEAR = 250
PROJECTION_YEARS = (1, 3)

# Phase 3 sizing assumption, as given: ~960 tickers x ~100 broker codes x
# ~250 trading days =~ 24M rows/year. We don't have real broker coverage
# numbers yet, so this stays a named constant, not a measurement.
BROKER_FLOW_TICKERS = 960
BROKER_FLOW_BROKER_CODES = 100
BROKER_FLOW_ROWS_PER_YEAR = BROKER_FLOW_TICKERS * BROKER_FLOW_BROKER_CODES * TRADING_DAYS_PER_YEAR

PRE_2020_CUTOFF = dt.date(2020, 1, 2)


def _pretty(n: int) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def fetch_total_db_size(conn) -> int:
    return conn.execute(text("select pg_database_size(current_database())")).scalar_one()


def fetch_table_sizes(conn) -> list[dict]:
    rows = conn.execute(
        text(
            """
            select
                c.relname as table_name,
                pg_table_size(c.oid) as data_bytes,
                pg_indexes_size(c.oid) as index_bytes,
                pg_total_relation_size(c.oid) as total_bytes
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = 'public' and c.relkind = 'r' and c.relname != 'alembic_version'
            order by pg_total_relation_size(c.oid) desc
            """
        )
    ).mappings().all()
    return [dict(r) for r in rows]


def fetch_row_counts(conn, table_names: list[str]) -> dict[str, int]:
    counts = {}
    for name in table_names:
        counts[name] = conn.execute(text(f'select count(*) from "{name}"')).scalar_one()
    return counts


def fetch_index_breakdown(conn, table_name: str) -> list[dict]:
    rows = conn.execute(
        text(
            """
            select indexrelname as index_name, pg_relation_size(indexrelid) as index_bytes
            from pg_stat_user_indexes
            where relname = :table_name
            order by pg_relation_size(indexrelid) desc
            """
        ),
        {"table_name": table_name},
    ).mappings().all()
    return [dict(r) for r in rows]


def fetch_ingest_runs_summary(conn) -> list[dict]:
    rows = conn.execute(
        text(
            """
            select job_name, count(*) as runs, sum(rows_written) as total_rows_written,
                   min(started_at) as first_run, max(finished_at) as last_run
            from ingest_runs group by job_name order by job_name
            """
        )
    ).mappings().all()
    return [dict(r) for r in rows]


def fetch_prices_daily_stats(conn) -> dict:
    """Empirical, measured growth basis: rows/trading-day and bytes/row,
    computed directly from what's actually in prices_daily — more
    representative of steady-state daily-job growth than ingest_runs
    history, which so far only contains one-time bulk backfill events
    (Phase 1a/1b), not daily-cadence runs."""
    total_rows, distinct_dates, pre_2020_yahoo_rows = conn.execute(
        text(
            """
            select
                count(*) as total_rows,
                count(distinct date) as distinct_dates,
                count(*) filter (where source = 'yahoo' and date < :cutoff) as pre_2020_yahoo_rows
            from prices_daily
            """
        ),
        {"cutoff": PRE_2020_CUTOFF},
    ).one()
    return {
        "total_rows": total_rows,
        "distinct_dates": distinct_dates,
        "avg_rows_per_date": total_rows / distinct_dates if distinct_dates else 0,
        "pre_2020_yahoo_rows": pre_2020_yahoo_rows,
    }


@app.command()
def main() -> None:
    engine = get_engine()
    with engine.connect() as conn:
        total_db_bytes = fetch_total_db_size(conn)
        table_sizes = fetch_table_sizes(conn)
        table_names = [t["table_name"] for t in table_sizes]
        row_counts = fetch_row_counts(conn, table_names)
        prices_index_breakdown = fetch_index_breakdown(conn, "prices_daily")
        ingest_summary = fetch_ingest_runs_summary(conn)
        pd_stats = fetch_prices_daily_stats(conn)

    # ---- measured: current state ----
    print("=" * 78)
    print("DATABASE SIZING REPORT (measured)")
    print("=" * 78)
    print(f"\nTotal database size: {_pretty(total_db_bytes)} ({total_db_bytes:,} bytes)\n")

    print(f"{'table':<20}{'rows':>12}{'data':>12}{'indexes':>12}{'total':>12}")
    for t in table_sizes:
        name = t["table_name"]
        print(
            f"{name:<20}{row_counts.get(name, 0):>12,}"
            f"{_pretty(t['data_bytes']):>12}{_pretty(t['index_bytes']):>12}"
            f"{_pretty(t['total_bytes']):>12}"
        )

    print("\nprices_daily index breakdown (largest table — where bloat would show up):")
    for idx in prices_index_breakdown:
        print(f"  {idx['index_name']:<40}{_pretty(idx['index_bytes']):>12}")

    print("\ningest_runs history (NOTE: bulk backfill events so far, not daily-cadence — see below):")
    for r in ingest_summary:
        print(
            f"  {r['job_name']:<28} runs={r['runs']:<4} "
            f"rows_written_total={r['total_rows_written'] or 0:>12,} "
            f"span={r['first_run']} -> {r['last_run']}"
        )

    print("\nprices_daily empirical density (measured — this is the projection basis):")
    print(f"  total rows:            {pd_stats['total_rows']:,}")
    print(f"  distinct dates:        {pd_stats['distinct_dates']:,}")
    print(f"  avg rows/date:         {pd_stats['avg_rows_per_date']:.0f}")
    print(f"  pre-2020 yahoo rows:   {pd_stats['pre_2020_yahoo_rows']:,}")

    # ---- derived: bytes/row, current-table growth basis ----
    prices_daily_row = next(t for t in table_sizes if t["table_name"] == "prices_daily")
    bytes_per_row = prices_daily_row["total_bytes"] / row_counts["prices_daily"]
    rows_per_trading_day_steady_state = 2 * BROKER_FLOW_TICKERS  # yahoo + idx, one bar each per active ticker
    bytes_per_trading_day = bytes_per_row * rows_per_trading_day_steady_state

    print(
        f"\nDerived: prices_daily averages {bytes_per_row:.1f} bytes/row (data+index). "
        f"Projections below assume ~{rows_per_trading_day_steady_state:,} new prices_daily "
        f"rows/trading day going forward (Yahoo + IDX, one bar per active ticker each) — "
        f"NOT the bulk-backfill rate seen in ingest_runs above."
    )

    # ---- projections ----
    print("\n" + "=" * 78)
    print("PROJECTIONS (bytes/row measured; row counts forward are estimates)")
    print("=" * 78)

    other_tables_bytes = total_db_bytes - prices_daily_row["total_bytes"]
    print(
        f"\nAll non-prices_daily tables today: {_pretty(other_tables_bytes)} "
        f"(treated as flat/negligible below — securities, trading_calendar, "
        f"annotation tables grow at human-entry pace, not daily-ingest pace)"
    )

    print("\n--- Scenario (i): current tables only ---")
    for years in PROJECTION_YEARS:
        added_rows = rows_per_trading_day_steady_state * TRADING_DAYS_PER_YEAR * years
        added_bytes = added_rows * bytes_per_row
        projected_total = total_db_bytes + added_bytes
        print(
            f"  +{years}yr: prices_daily +{added_rows:,} rows (+{_pretty(added_bytes)}) "
            f"-> total DB ~{_pretty(projected_total)}"
        )

    print(
        f"\n--- Scenario (ii): (i) + Phase 3 broker_flow_daily ---\n"
        f"  ANALYTICAL ESTIMATE — broker_flow_daily has no data yet. Per-row bytes "
        f"proxied from prices_daily's measured {bytes_per_row:.1f} bytes/row (similar "
        f"column count/shape); real number could differ once it exists.\n"
        f"  Assumption: {BROKER_FLOW_TICKERS} tickers x {BROKER_FLOW_BROKER_CODES} broker codes "
        f"x {TRADING_DAYS_PER_YEAR} trading days = {BROKER_FLOW_ROWS_PER_YEAR:,} rows/year"
    )
    for years in PROJECTION_YEARS:
        prices_added_rows = rows_per_trading_day_steady_state * TRADING_DAYS_PER_YEAR * years
        prices_added_bytes = prices_added_rows * bytes_per_row
        broker_rows = BROKER_FLOW_ROWS_PER_YEAR * years
        broker_bytes = broker_rows * bytes_per_row
        projected_total = total_db_bytes + prices_added_bytes + broker_bytes
        print(
            f"  +{years}yr: prices_daily +{_pretty(prices_added_bytes)}, "
            f"broker_flow_daily +{broker_rows:,} rows (+{_pretty(broker_bytes)}) "
            f"-> total DB ~{_pretty(projected_total)}"
        )

    print("\n--- Variant: pre-2020 Yahoo history EXCLUDED from production set ---")
    pre_2020_fraction = pd_stats["pre_2020_yahoo_rows"] / pd_stats["total_rows"]
    pre_2020_bytes = pd_stats["pre_2020_yahoo_rows"] * bytes_per_row
    print(
        f"  {pd_stats['pre_2020_yahoo_rows']:,} rows ({pre_2020_fraction:.1%} of prices_daily) "
        f"=~ {_pretty(pre_2020_bytes)} saved today (prices_daily table+index, estimated "
        f"from measured avg bytes/row — homogeneous schema, so this should track closely)."
    )
    for years in PROJECTION_YEARS:
        prices_added_bytes = rows_per_trading_day_steady_state * TRADING_DAYS_PER_YEAR * years * bytes_per_row
        projected_total_i_excl = total_db_bytes - pre_2020_bytes + prices_added_bytes
        broker_bytes = BROKER_FLOW_ROWS_PER_YEAR * years * bytes_per_row
        projected_total_ii_excl = projected_total_i_excl + broker_bytes
        print(
            f"  +{years}yr: scenario (i) ~{_pretty(projected_total_i_excl)}, "
            f"scenario (ii) ~{_pretty(projected_total_ii_excl)}"
        )


if __name__ == "__main__":
    app()
