# idx-data

IDX point-in-time-correct data ingestion pipeline. Spec: [idx-data-pipeline-spec.md](idx-data-pipeline-spec.md).

## Status

### Phase 0 — done
Schema + migrations + one ticker end-to-end. `AMMN` has full price history in Postgres.

### Phase 1a — done
Full backfill of the **current, active** IDX universe.

- `securities`: 962 active tickers seeded from IDX's `GetCompanyProfiles`
  directory (`jobs/seed_universe.py`).
- `prices_daily` (source='yahoo'): all 962 backfilled via `jobs/bootstrap.py`
  (checkpointed — safe to kill and resume).
- `trading_calendar`: derived heuristically (≥30% of tickers with a
  non-null close, spec §3.1 step 5) from 2000-03-30 to today. Phase 1b
  overwrites 2020-01-02+ with ground truth (see below).

**Validated (2026-08-06):**
| Check | Result |
|---|---|
| Duplicate `(ticker, date, source)` | 0 |
| Tickers with ≥1yr history | 904 / 962 (93.97% raw; 95.06% excluding 11 tickers too recently listed to possibly qualify) |
| Tickers with zero Yahoo data | 7 (old listings — e.g. META listed 2001 — Yahoo simply has nothing) |
| Tickers with a single stale quote only | 40 — IDX names under **extended trading suspension** (e.g. `WSKT`/Waskita Karya, mid debt restructuring); Yahoo returns one current-ish quote instead of history. Structural, not fixable from our side. |
| OHLC sanity violations (`close` outside `[low, high]`, etc.) | 118 / 3.48M rows (0.003%) — concentrated on **2007-01-10** and **2007-02-02**, hitting 9+ unrelated tickers each on the same two dates. Looks like an upstream Yahoo feed defect on those specific dates, not random noise or a pipeline bug. Flagged for `jobs/validate.py` (not yet built) to exclude explicitly. |

**⚠️ Survivorship bias: `securities` is active-listings-only until Phase 1b's
`reconcile-delisted` step runs.** Spec §0 principle 4 exists because of
exactly this — do not train or backtest against `securities`/`prices_daily`
as if this were the full point-in-time universe yet.

### Phase 1b — in progress
Delisted-tail discovery + IDX raw-price cross-check, via
`jobs/harvest_universe_history.py` walking IDX's `GetStockSummary` daily
endpoint (writes `prices_daily` source='idx') and then diffing the ticker
union against Phase 1a's active list (`reconcile-delisted` subcommand).

**Known, permanent limitation, not a bug to fix:** the original goal was
delisted-tail coverage back to ~2010. `GetStockSummary` has **no data
before 2020-01-02** — every date probed in 2010–2019 returns `recordsTotal:
0` with a clean 200 OK. This is IDX's own retention wall, discovered
empirically; no endpoint we found serves further back. **Tickers delisted
before 2020-01-02 remain invisible to this pipeline.** Closing that gap
needs a different source (IDX delisting-announcement archive, a paid
vendor, or manual compilation) — not attempted yet.

Once `reconcile-delisted` has run, this section will state exactly how many
delisted tickers were recovered and the date range they cover.

## Local dev setup

```bash
# 1. Postgres (dev only — Railway is the system of record per spec §1)
docker run -d --name idx-postgres \
  -e POSTGRES_USER=idx -e POSTGRES_PASSWORD=idx_dev_local -e POSTGRES_DB=idx \
  -p 5432:5432 -v idx_postgres_data:/var/lib/postgresql/data postgres:16

# 2. Python env
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 3. Config
cp .env.example .env   # defaults already match the docker command above

# 4. Schema
alembic upgrade head

# 5. Seed the active universe, then backfill everyone
python -m idx.jobs.seed_universe
python -m idx.jobs.bootstrap                # all active tickers, checkpointed/resumable
python -m idx.jobs.bootstrap --tickers AMMN # just one, e.g. for a quick smoke test

# 6. Historical harvest (delisted-tail discovery + IDX raw price cross-check)
python -m idx.jobs.harvest_universe_history harvest    # 2020-01-02 -> today, resumable
python -m idx.jobs.harvest_universe_history reconcile-delisted

# Tests
pytest tests/ -q
```

`seed/securities_seed.csv` is now only a dev/override path (`bootstrap.py
--seed-csv`) — `jobs/seed_universe.py` is what normally populates
`securities`.

## Rate limiting / ToS

Both `sources/idx_company_list.py` and `sources/idx_official.py` hit
IDX's own (Cloudflare-protected, undocumented) internal JSON API via
`curl_cffi` browser impersonation. Requests are paced (~1.2s between
calls) and sequential, not parallelized. This is for personal research /
model training use — IDX's terms restrict commercial redistribution of
scraped data; if this ever serves IDX-derived data to other people, license
that properly first.
