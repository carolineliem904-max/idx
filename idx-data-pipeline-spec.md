# IDX Data Ingestion Pipeline — Build Spec

**Role:** Builder (Claude Code). This spec is the contract. Ask before deviating.
**Owner/validator:** Caroline
**Goal:** A point-in-time-correct IDX dataset suitable for ML, with human-annotated ownership, broker-flow, and rumor overlays.

---

## 0. Non-negotiable principles

These exist because violating them silently produces a model that backtests beautifully and loses money live.

1. **Point-in-time correctness.** Every row carries both *when the fact was true* (`valid_from` / `valid_to`) and *when we learned it* (`created_at` / `ingested_at`). The feature builder may only read rows where `created_at <= as_of`. No exceptions.
2. **Append, never overwrite.** Price revisions are appended as new rows with a later `ingested_at`; the latest wins in views, but history is preserved for audit.
3. **Store raw and adjusted prices side by side.** Raw for microstructure work (tick size, ARA/ARB bands), adjusted for return series.
4. **Universe is a time series, not a list.** Delisted tickers stay in the database with `delisting_date` set. A backtest that only sees survivors is worthless.
5. **Idempotency.** Any job can be re-run for any date range without corrupting state.

---

## 1. Stack

- Python 3.11+
- Postgres (Railway) as system of record
- SQLAlchemy 2.x + Alembic for migrations
- `yfinance` for price backfill and daily EOD
- `pandas` + `pyarrow` for Parquet cold storage / fast feature builds
- Railway cron (or GitHub Actions cron as free fallback) for scheduling
- `structlog` for JSON logs; every job writes an `ingest_runs` row

Directory layout:

```
idx-data/
  alembic/
  src/idx/
    db/models.py
    db/session.py
    sources/yahoo.py
    sources/idx_official.py      # phase 2+
    sources/sectors.py           # phase 3+, broker flow
    jobs/bootstrap.py
    jobs/daily.py
    jobs/validate.py
    annotate/cli.py
    features/builder.py
  tests/
  pyproject.toml
```

---

## 2. Schema

### 2.1 Core market data

```sql
securities (
  ticker            text primary key,        -- 'AMMN' (no .JK suffix in DB)
  yahoo_symbol      text not null,           -- 'AMMN.JK'
  name              text,
  sector            text,
  sub_industry      text,
  listing_date      date,
  delisting_date    date,                    -- null = still listed
  board             text,                    -- utama / pengembangan / ekonomi baru / pemantauan khusus
  is_active         boolean not null default true,
  created_at        timestamptz not null default now()
)

prices_daily (
  ticker            text references securities(ticker),
  date              date not null,
  open_raw          numeric(18,4),
  high_raw          numeric(18,4),
  low_raw           numeric(18,4),
  close_raw         numeric(18,4),
  close_adj         numeric(18,6),
  volume            bigint,                  -- shares, not lots
  value_traded      numeric(20,2),           -- IDR, if available
  frequency         integer,                 -- number of trades, if available
  source            text not null,           -- 'yahoo' | 'idx' | 'sectors'
  ingested_at       timestamptz not null default now(),
  primary key (ticker, date, source)
)

corporate_actions (
  id                bigserial primary key,
  ticker            text references securities(ticker),
  ex_date           date not null,
  action_type       text not null,           -- split | reverse_split | dividend | rights | bonus | warrant
  ratio_from        numeric,
  ratio_to          numeric,
  cash_amount       numeric,
  source            text,
  created_at        timestamptz not null default now()
)

trading_calendar (
  date              date primary key,
  is_trading_day    boolean not null,
  note              text                     -- 'Idul Fitri', 'cuti bersama', half-day, etc.
)
```

### 2.2 Annotation layer — Caroline's edge

This is the part no vendor sells. Treat it as first-class data with the same rigor as prices.

```sql
entities (
  id                bigserial primary key,
  name              text not null,           -- 'Grup X', 'Bandar Y', 'Broker YP'
  entity_type       text not null,           -- ultimate_owner | operator | broker | fund | family_group
  broker_code       text,                    -- 'YP', 'CC', 'AK' — only for entity_type='broker'
  notes             text,
  created_at        timestamptz not null default now()
)

security_control (
  id                bigserial primary key,
  ticker            text references securities(ticker),
  entity_id         bigint references entities(id),
  role              text not null,           -- ultimate_owner | nominee | suspected_operator | active_accumulator
  valid_from        date not null,           -- when you believe this became true
  valid_to          date,                    -- null = still true
  confidence        smallint not null,       -- 1..5, 5 = documented in filing
  evidence          text,                    -- KSEI filing URL, news link, or 'personal knowledge'
  created_at        timestamptz not null default now(),   -- WHEN YOU RECORDED IT
  unique (ticker, entity_id, role, valid_from)
)

rumors (
  id                bigserial primary key,
  ticker            text references securities(ticker),
  heard_at          timestamptz not null,    -- when it reached YOU, not when it became true
  source_channel    text,                    -- telegram_grup_a | broker_call | twitter | private
  source_reliability smallint,               -- 1..5, calibrate this over time
  direction         text,                    -- bullish | bearish | neutral
  specificity       text,                    -- vague | targeted_price | named_catalyst | dated_event
  body              text not null,
  entity_id         bigint references entities(id),  -- optional: whose rumor
  resolved_at       timestamptz,
  outcome           text,                    -- true | false | partial | unresolved
  created_at        timestamptz not null default now()
)

broker_flow_daily (                          -- phase 3, from Sectors or IDX broker summary
  ticker            text references securities(ticker),
  date              date not null,
  broker_code       text not null,
  buy_volume        bigint,
  sell_volume       bigint,
  buy_value         numeric(20,2),
  sell_value        numeric(20,2),
  investor_type     text,                    -- domestic | foreign | all
  source            text not null,
  ingested_at       timestamptz not null default now(),
  primary key (ticker, date, broker_code, investor_type, source)
)
```

### 2.3 Audit

```sql
ingest_runs (
  id                bigserial primary key,
  job_name          text not null,
  started_at        timestamptz not null,
  finished_at       timestamptz,
  status            text,                    -- success | partial | failed
  rows_written      integer,
  tickers_attempted integer,
  tickers_failed    integer,
  error_summary     text
)
```

---

## 3. Jobs

### 3.1 `jobs/bootstrap.py` — one-time full backfill

1. Build the `securities` table. Seed from IDX's listed-company list; also read any locally saved historical listing/delisting announcements so delisted tickers are present.
2. For each ticker, fetch full available history from Yahoo (`period="max"`, `interval="1d"`, `auto_adjust=False` so both raw and adjusted are retained).
3. Batch in groups of ~50 symbols with a 1–2 s pause between batches. Yahoo throttles aggressively.
4. Write to `prices_daily` with `source='yahoo'`.
5. Derive `trading_calendar` from the union of dates where ≥30% of active tickers have non-null closes; then hand-review and annotate holidays.
6. Snapshot everything to Parquet under `data/cold/prices_daily/date=YYYY-MM-DD/`.

Acceptance: ≥95% of currently listed tickers have ≥1 year of history; no duplicate `(ticker, date, source)`; row counts logged in `ingest_runs`.

### 3.2 `jobs/daily.py` — incremental update

**Schedule:** 18:30 WIB (11:30 UTC), Monday–Friday. IDX closes ~16:00 WIB; Yahoo typically settles `.JK` EOD within 1–3 hours. 18:30 gives margin.

Logic:

1. Check `trading_calendar`. If today is not a trading day, log a skipped run and exit 0.
2. Fetch a **rolling 7-calendar-day window**, not just yesterday. Yahoo revises and occasionally back-fills late. The 7-day window catches corrections for free.
3. Upsert into `prices_daily` on `(ticker, date, source)`. If values differ from the existing row, write a new row with a fresh `ingested_at` rather than mutating.
4. Run `jobs/validate.py` inline; if it fails hard, mark the run `partial` and alert.
5. Append the day's slice to Parquet.

Retry policy: 3 attempts per batch, exponential backoff (5 s, 20 s, 60 s). Individual ticker failures do not fail the run — they increment `tickers_failed` and are retried next day by the rolling window.

**Fallback if Yahoo breaks** (it will, periodically): the job should be source-agnostic behind `sources/base.py::PriceSource`. Adding `sources/idx_official.py` (daily *Ringkasan Saham* XLSX) or a paid API later must not require touching `jobs/daily.py`.

### 3.3 `jobs/validate.py` — data quality gate

Fail loudly on:

- Zero rows written on a trading day
- `>10%` of active tickers missing today's bar
- Any `close_raw` change `>35%` day-over-day without a matching `corporate_actions` row (IDX auto-reject bands make this near-impossible organically — it almost always means an unadjusted split)
- `high_raw < low_raw`, or close outside `[low, high]`
- `volume = 0` for a ticker whose 20-day median volume is in the top 300
- Any `security_control` or `rumors` row where `created_at < valid_from`/`heard_at` (impossible — indicates backdating, which destroys the leakage guard)

---

## 4. Annotation workflow

`annotate/cli.py` — a small Typer CLI so recording knowledge takes seconds, not minutes. If it's slow, it won't get used, and unused annotations are worthless.

```
idx entity add --name "Grup X" --type family_group
idx control add --ticker COCO --entity 3 --role suspected_operator \
    --from 2025-11-01 --confidence 3 --evidence "personal knowledge"
idx rumor add --ticker ARKO --direction bullish --specificity named_catalyst \
    --source telegram_grup_a --reliability 2 --body "..."
idx rumor resolve --id 17 --outcome false
```

Rules enforced by the CLI:

- `created_at` is always `now()` and cannot be passed as an argument.
- `valid_from` may be backdated (you may only now be confident about something that started in March), but the `created_at` gap is preserved and the feature builder respects it.
- `heard_at` defaults to `now()` and may be backdated at most 30 days, with a warning.

**Calibration loop:** once ~50 rumors are resolved, compute hit-rate per `source_channel` and per `source_reliability` level. If a channel's hit rate is ~50% on directional calls, it's noise — stop feeding it to the model. This alone is worth building.

---

## 5. Feature builder

`features/builder.py` exposes one function:

```python
def build(as_of: date, lookback_days: int = 252) -> pd.DataFrame:
    """Returns one row per (ticker, date) using ONLY information
    knowable at `as_of`. Every table is filtered on created_at <= as_of."""
```

Feature families:

**Price/volume (from `prices_daily`)** — returns over 1/5/20/60d, realized vol, volume z-score, distance from 20/60d MA, Amihud illiquidity, days-since-last-zero-volume.

**Entity behavior (from `security_control` + `prices_daily`)** — for each entity, aggregate the *historical* behavior of every stock it controlled, computed strictly before `as_of`:
- median duration from `valid_from` to peak price
- median max drawdown during control period
- median return in the 60 days after control began
- number of prior episodes (this is your sample-size warning flag)

Then join those entity-level statistics onto currently-controlled tickers. This is the "does this operator have a repeatable playbook" question, expressed as features.

**Broker flow (phase 3)** — net foreign flow 5/20d, concentration ratio (top-3 brokers' share of daily volume), accumulation streak length, divergence between broker net-buy and price.

**Rumor (from `rumors`)** — count in trailing 5/20d, reliability-weighted directional score, days since last rumor, and the *source's historical hit rate as of that date* (not overall — that would leak).

### Warnings the Builder must surface, not hide

- Entity-level features will have tiny `n`. Ten entities × three episodes each is not a dataset. Apply hierarchical shrinkage toward the global mean and **always emit `n_episodes` as a feature** so the model can discount thin evidence.
- Rumor features will be sparse and only cover a handful of tickers. Do not impute zeros as "no rumor" without a companion `has_rumor_coverage` flag — the absence of a rumor for TLKM means something different than for COCO.

---

## 6. Modeling guardrails (phase 5, but design for them now)

- **Walk-forward validation with an embargo.** Train on `[t-3y, t]`, purge 10 days, test on `[t+10d, t+3m]`. Roll forward. No random k-fold — it leaks across time.
- **Liquidity filter before labeling.** Drop ticker-days with 20-day median value traded below ~IDR 1–2 bn. Otherwise the backtest fills at prices that never existed.
- **ARA/ARB awareness.** IDX auto-reject bands censor the daily return distribution, and the rules have been revised multiple times. Store the band regime per date in `trading_calendar` (add a `ara_band` / `arb_band` column) and either clip labels accordingly or exclude limit-hit days from training.
- **Baseline first.** Gradient-boosted trees on the cross-section. If a deep sequence model can't beat it on walk-forward, it isn't beating it.

---

## 7. Phasing

| Phase | Deliverable | Done when |
|---|---|---|
| 0 | Schema + migrations + one ticker end-to-end | `AMMN` has full history in Postgres |
| 1 | Full backfill, all tickers, Parquet snapshot | Validation suite passes on bootstrap |
| 2 | Daily cron on Railway + alerting | 10 consecutive green runs |
| 3 | Annotation CLI + first 20 entities recorded | Caroline can log a rumor in <30 s |
| 4 | Feature builder with point-in-time tests | Leakage test suite passes (see below) |
| 5 | Baseline model + walk-forward harness | Honest OOS Sharpe reported, good or bad |

**Required leakage test (phase 4):** construct a synthetic annotation with `valid_from = 2024-01-01` and `created_at = 2025-06-01`. Assert that `build(as_of=date(2024, 6, 1))` does not contain it. This test failing is the single most likely way this project quietly produces garbage.

---

## 8. Notes for the Builder

- Never hardcode `.JK`. Map through `securities.yahoo_symbol` so a source swap is a config change.
- All timestamps stored in UTC; convert to WIB only at display.
- IDX lot size is 100 shares. Yahoo reports volume in shares. Be explicit in column names about which unit is stored.
- Secrets via environment variables only. No API keys in the repo.
- Every job must be runnable locally with `python -m idx.jobs.daily --date 2026-08-04 --dry-run`.
