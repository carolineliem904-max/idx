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

### Phase 1b — done (within its documented bound)
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

**Validated (2026-08-06):**
| Check | Result |
|---|---|
| Trading days harvested | 1,721 dates attempted, 0 failed (2020-01-02 → 2026-08-05), 1,585 confirmed trading days |
| `prices_daily` (source='idx') | 1,336,016 rows, 989 tickers, 0 duplicate PK, 0 OHLC sanity violations |
| Delisted tickers recovered | **27** — see table below. All last-observed between 2020-01-17 and 2026-08-05, consistent with the harvest's own coverage window (nothing earlier is possible to find, by construction) |
| `trading_calendar` | 2020-01-02+ now ground-truth from IDX (verified/weekend/holiday, no threshold); pre-2020 stays on bootstrap's heuristic |

`securities` is now 989 rows total: 962 active (Phase 1a) + 27 inferred-delisted.
**Still survivorship-biased for anything delisted before 2020-01-01** — that
gap is real and open, not fixed by this phase. `delisting_date` for the 27
is *last observed trading via IDX*, not an official delisting-announcement
date (spec §3.1 step 1's ideal source) — treat it as a proxy, not ground truth.

<details>
<summary>The 27 recovered delisted tickers</summary>

`GOTOM` (2026-08-05), `CNTX`/`CNTB` (2026-07-29), `MASA` (2025-10-29),
`MFIN` (2025-10-01), `KRAH`/`HDTX`/`MYRXP`/`KPAL`/`KPAS`/`JKSW`/`MAMI`/
`NIPS`/`PRAS`/`FORZ`/`MYRX`/`MAMIP` (2025-07-18), `FREN` (2025-04-16),
`RMBA` (2024-01-15), `TURI` (2023-04-05), `FINN` (2021-05-04), `GREN`
(2020-11-20), `CKRA` (2020-08-27), `SCBD` (2020-04-17), `APOL`
(2020-04-03), `ITTG` (2020-01-22), `BORN` (2020-01-17) — dates are last
observed trading, not official delisting dates.
</details>

**Two real bugs found and fixed during this phase**, both from the same
root cause (concurrent jobs writing shared tables without coordinating who
wins):
1. The harvester's FK-satisfying placeholder insert let SQLAlchemy's
   column default silently set every newly-discovered ticker
   `is_active=True` — which made `reconcile-delisted`'s diff against the
   active universe empty by construction (every ticker it ever touched
   looked active). Fixed with `db/upserts.py::ensure_security_placeholder`,
   which never sets `is_active` on an existing row and defaults new ones to
   `False` explicitly rather than relying on the column default.
2. Running the harvester and `bootstrap.py` concurrently let bootstrap's
   blanket heuristic `trading_calendar` upsert overwrite already-correct
   IDX ground-truth rows for every date it had covered by the time
   bootstrap finished. Fixed with `resync-calendar`, which re-derives
   trading_calendar for the harvested range from already-persisted data —
   safe to rerun any time this drift is suspected.

### Phase 2 — schema fix done; daily ingestion + validation + alerting built and validated locally; **stays OPEN** (see "Local scheduling")

Deployment deferred by explicit decision: local Docker Postgres throughout,
no Railway. Everything routes through `DATABASE_URL` — deploying later
should be a config change, not a code change.

**Schema fix, before any of Part A-G:** designing `jobs/daily.py` surfaced
a real contradiction between spec §2.1's schema (`prices_daily` PK on
`(ticker, date, source)` — one row per key, ever) and spec §3.2's own
stated behavior ("write a new row with a fresh `ingested_at` rather than
mutating... history is preserved for audit") — the old PK couldn't
physically hold two versions of a trading day. Widened the PK to
`(ticker, date, source, ingested_at)`, added `prices_daily_latest`
(`DISTINCT ON ... ORDER BY ingested_at DESC`) as the "current state" read
path, and `db/queries.py::price_as_of` as the actual leakage guard every
point-in-time consumer (Phase 4's feature builder included) must use.
`db/upserts.py::upsert_price_bar` now writes a new row **only** when a
value is new or has actually changed — proven live, not just by unit
test: a real Yahoo revision to AMMN's 2026-08-06 close (captured mid-day
before settlement, corrected two days later) is preserved as two rows,
and an as-of read between the two timestamps correctly returns the
original value. Spec doc §2.1/§3.2 updated to match.

**A — Sizing (`jobs/db_stats.py`):** measured 644MB total, `prices_daily`
is 633.5MB of it (418MB data + 215MB index). Projected: current tables
alone ~716MB in 1yr, ~861MB in 3yr — comfortable. **Phase 3's
`broker_flow_daily` changes that by an order of magnitude**: even the
original worst-case estimate (24M rows/yr) alone projects ~3.5GB in year
one. See "Phase 3 (not started)" below for why that number itself isn't
trustworthy yet and what has to happen before it drives a hosting
decision. A real bug came out of just running this: `securities` (989
rows) had bloated to **84MB** from Postgres's `ON CONFLICT DO NOTHING`
performing a speculative insertion even when a row already exists —
~1.6M redundant harvester calls left ~1.6M dead tuples. Reclaimed via
`VACUUM FULL` (84MB → 144KB) and fixed at the source with a known-tickers
cache so it can't silently recur.

**B — Backups (`jobs/backup.py`, `make backup`):** compressed `pg_dump
-Fc`, timestamped, gitignored `data/backups/`, retention = most recent 7
distinct days + 4 distinct ISO weeks (verified against 35 synthetic
backups — kept exactly the 9 files the math predicts). `make verify`
actually restores into a throwaway database and compares row counts
against live — run for real: securities 989/989, prices_daily
4,204,533/4,204,533, trading_calendar 9,626/9,626, ingest_runs 14/14, full
match. Provider-neutral (tries real `pg_dump`/`pg_restore` first, falls
back to `docker exec` only because Postgres is Docker-only locally right
now) — no Railway-specific code. Full local backups intentionally include
pre-2020 Yahoo history (exploration data, still worth protecting
locally); a scoped production export using `PRODUCTION_DATA_CUTOFF` is a
future deployment-time task, not built now.

**C — `jobs/daily.py`:** both sources every run (Yahoo, per-ticker range
fetch; IDX, per-day all-tickers fetch via
`harvest_universe_history.py::harvest_one_day` reused directly, not
reimplemented — same publish-lag grace window, same resumable-skip
check). Rolling 7-day window, trading_calendar checked first, retry 3x
backoff 5/20/60s. Validated against real data: a fully-idempotent rerun
of an already-settled day (0 new/revised writes), a correct weekend skip,
and a live run that found 2 genuinely new bars, 2 genuine late-Yahoo
revisions, and resolved 2 previously publish-lag-undetermined IDX dates —
none of it staged.

**D — `known_issues` + `jobs/validate.py`:** suppression built *before*
alerting, per instruction — "I'll start ignoring the channel within a
week and then miss a real failure" otherwise. Implements all 6 spec §3.3
checks plus a 7th (`check_insufficient_history`) added because Phase 1a
found a real pattern none of the original 6 catch (IDX names under
extended suspension). Every finding is checked against `known_issues`;
suppressed findings are always still printed, in a separate "known,
suppressed" section — never silently dropped. `jobs/seed_known_issues.py`
seeded the two Phase 1 findings (2007 OHLC-anomaly dates; suspended
tickers, currently 47, re-derived live from the DB rather than
hand-typed so it can't drift from what the check itself finds). **Real
bug caught on the first live run:** `missing_bar_pct` is an aggregate
check with no per-ticker `Finding`, so a per-ticker suppression could
never match it — the 47 chronically-missing tickers would have inflated
it toward the 10% threshold every single day, forever. Fixed by excluding
known-insufficient-history tickers from the check's own denominator
directly.

**E — Cross-source reconciliation (`jobs/reconcile.py`):** flags Yahoo vs
IDX `close_raw` disagreement beyond **one IDX tick** (real fraksi harga
schedule, not an arbitrary tolerance — spec §6 itself notes tick rules
have been revised multiple times, so this is knowingly imprecise for
older dates) into `price_discrepancies`. Two purposes: a permanent canary
for the next 2007-style upstream defect, and systematic per-ticker
disagreement usually means an unhandled corporate action — exactly the
leakage that ruins backtests quietly. Found 256 real discrepancies on
2026-08-06 (explained: most of the universe's Yahoo snapshot for that
date was still the pre-settlement capture) and one standing candidate
worth a look later: `FASW` shows an identical Yahoo close across 5
consecutive days against a different, also-constant IDX close.

**F — Alerting (`notify.py`, `alerting.py`, `jobs/dead_mans_switch.py`):**
`Notifier` ABC + `ConsoleNotifier` — adding Telegram later is one class,
selected via `IDX_NOTIFIER` env var, zero rework in callers. Rules: run
failed/partial, >10% missing bars (called out separately from generic
validator failures for visibility), any other non-suppressed validator
failure, discrepancies above threshold (5, a first guess — not measured),
plus a short "OK" summary on a clean run so the channel shows the
pipeline is alive. **Dead man's switch is a standalone job**, deliberately
not called from inside `daily.py` — a check for "daily.py hasn't run"
that only runs as part of daily.py running can never fire when it
matters. Checks `ingest_runs` for a successful `daily` row within 36h.

**G — Local scheduling (launchd, `launchd/*.plist`):** ⚠️ **this is a
development stand-in, not production infrastructure.** `launchctl`
+ `StartCalendarInterval` (not cron — it runs missed jobs when the
machine wakes from sleep, which cron does not). Two jobs:
`com.idx.daily` (weekdays 18:30 *local time* — **only correct if this
Mac's system timezone is Asia/Jakarta**; the plist documents converting
11:30 UTC otherwise) and `com.idx.deadmansswitch` (every 6h, independent
schedule). See "Local scheduling" below for load/unload and log
locations. **Spec §7's "10 consecutive green runs" acceptance criterion
does not meaningfully accrue on a laptop that sleeps, loses power, or is
simply off overnight — Phase 2 stays open until this runs on real
infrastructure.** Not marked done.

**Phase 3 (not started) — open questions, decided now so they aren't a retrofit:**
- **`broker_flow_daily`'s sizing estimate is a worst case, not a mean**
  (960 tickers × 100 broker codes × 250 days ≈ 24M rows/yr) — flagged
  explicitly: only ~650 tickers trade on a typical day (measured), and
  broker-code counts per ticker are heavily skewed (liquid names 80+,
  thin names 5-10; realistic average likely 15-30, nearer 4M rows/yr).
  **First Phase 3 task, before anything else**: fetch one real day of IDX
  broker summary, measure the actual distinct-broker-codes-per-ticker
  distribution (min/median/p90/max, and how many tickers appear at all),
  reproject from *that*. Hosting gets decided on measured numbers, not
  the current placeholder.
- **`broker_flow_daily` should be scoped to a liquidity-filtered subset**
  (~top 250 by trailing median value traded), not the full universe —
  designed now, not implemented. This is the same ranking spec §6's own
  modeling guardrail already calls for ("drop ticker-days with 20-day
  median value traded below ~IDR 1-2bn"), and the same query shape
  `jobs/validate.py::check_zero_volume_top300` already implements (there,
  ranked by median *volume*; the Phase 3 filter ranks by median *value
  traded* instead — same pattern, different column). When Phase 3 starts,
  this becomes a `top_n_by_value_traded()` helper the broker-flow
  ingestion job calls before deciding what to fetch, not a full-universe
  fetch filtered after the fact.

## Local scheduling (launchd — development stand-in)

```bash
# Load (starts the recurring schedule immediately)
launchctl load ~/Library/LaunchAgents/com.idx.daily.plist       # symlink or copy from launchd/
launchctl load ~/Library/LaunchAgents/com.idx.deadmansswitch.plist

# Check status / next run
launchctl list | grep com.idx

# Unload (stops it)
launchctl unload ~/Library/LaunchAgents/com.idx.daily.plist
launchctl unload ~/Library/LaunchAgents/com.idx.deadmansswitch.plist
```

Copy (don't move) the two `.plist` files from `launchd/` into
`~/Library/LaunchAgents/` before loading — launchd only looks there.
**Before loading `com.idx.daily.plist`**, confirm this Mac's system
timezone; the plist's `Hour`/`Minute` values (18:30) are only correct for
Asia/Jakarta (WIB) — see the comment inside the file for the UTC
reference point and what to edit otherwise. If this repo is at a
different path than `/Users/carolineliem/Documents/idx-data`, update the
absolute paths in both `.plist` files and both `scripts/run_*.sh`
wrappers first.

Logs: `data/logs/launchd/` — one timestamped file per run
(`daily_YYYYMMDD_HHMMSS.log`, `dead_mans_switch_YYYYMMDD_HHMMSS.log`),
plus `daily_stdout.log`/`daily_stderr.log` (and the dead-man's-switch
equivalents) for anything that happens outside the Python process itself
(e.g. the venv failing to start at all).

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
python -m idx.jobs.harvest_universe_history resync-calendar  # only if harvest ran concurrently with bootstrap

# 7. Known issues + validation + daily incremental update
python -m idx.jobs.seed_known_issues
python -m idx.jobs.validate --start 2026-08-01 --end 2026-08-07   # ad-hoc audit; exits 1 on a real failure
python -m idx.jobs.daily                        # today's incremental update, both sources
python -m idx.jobs.daily --date 2026-08-04 --dry-run

# 8. Backups
make backup                                      # pg_dump -Fc, timestamped, retention-pruned
make backup-list
make verify                                       # actually restores into a throwaway DB and checks it
make restore FILE=data/backups/idx_backup_....dump TARGET=idx_restored

# 9. Dead man's switch (normally scheduled independently — see "Local scheduling")
python -m idx.jobs.dead_mans_switch

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
