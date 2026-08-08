# CLAUDE.md

Guidance auto-loaded every session. Keep this current, permanent, and free
of anything that goes stale fast — that's what HANDOFF.md is for.

## What this is

IDX (Indonesia Stock Exchange) point-in-time-correct data ingestion
pipeline. Full contract: [idx-data-pipeline-spec.md](idx-data-pipeline-spec.md).
Read it before touching schema or job behavior — it's the spec, not a
suggestion. Where the spec and the code have ever disagreed, the
resolution is recorded in the spec doc itself (see its inline notes on
`prices_daily`'s PK) — the spec should never silently drift from what's
actually built.

## Stack

- Python 3.11+ (dev: 3.13.5 in `.venv`)
- Postgres — local Docker for dev; Railway deliberately deferred (see HANDOFF.md)
- SQLAlchemy 2.x + Alembic
- `yfinance` (Yahoo prices) + `curl_cffi` (IDX's own Cloudflare-protected internal JSON API)
- `pandas` + `pyarrow` (Parquet cold storage)
- `structlog` (JSON logs), `typer` (every job is a CLI)

## Layout

```
idx-data/
  alembic/versions/          migrations — hand-write anything autogenerate can't diff (PK-only changes, views)
  launchd/                   macOS launchd plists — dev scheduling stand-in, see HANDOFF.md
  scripts/                   launchd wrapper shells (per-run timestamped logs)
  seed/                      dev-only CSV (bootstrap.py --seed-csv override path)
  src/idx/
    config.py                 DATABASE_URL, PRODUCTION_DATA_CUTOFF, local_pg_container()
    notify.py                 Notifier ABC + ConsoleNotifier (get_notifier() picks backend)
    alerting.py                daily.py's alert rules, separate from validate.py's checks
    db/
      models.py                ORM, mirrors spec §2 + Phase 2 extensions (known_issues, price_discrepancies)
      session.py                 session_scope() context manager — commits on success, rolls back on error
      upserts.py                  shared idempotent upsert helpers — read the docstrings before touching
      queries.py                   point-in-time reads: latest_price(), price_as_of()
    sources/
      base.py                    PriceSource ABC
      yahoo.py                    YahooSource
      idx_company_list.py          IDX GetCompanyProfiles (current listed directory only)
      idx_official.py               IDX GetStockSummary (daily trading summary / "Ringkasan Saham")
    jobs/                       every job: typer CLI, `python -m idx.jobs.<name> --help`
      bootstrap.py               one-time full backfill (Phase 0/1a)
      seed_universe.py            Phase 1a: active universe from IDX's own directory
      harvest_universe_history.py Phase 1b: historical IDX harvest, delisted-tail discovery
      daily.py                    Phase 2: incremental daily update, both sources
      validate.py                 data quality gate, known_issues-aware
      seed_known_issues.py        seeds known_issues from confirmed findings
      reconcile.py                 cross-source (Yahoo vs IDX) discrepancy detection
      dead_mans_switch.py          standalone staleness check — NOT called from daily.py
      backup.py                   pg_dump/restore/verify
      db_stats.py                  sizing diagnostics
  tests/
  Makefile                     make backup / backup-list / verify / restore / test
```

`annotate/` and `features/` from the spec's original directory sketch
don't exist yet — Phase 3/4, not started.

## Running things locally

```bash
docker start idx-postgres   # or the full `docker run ...` in README if it doesn't exist yet
source .venv/bin/activate
python -m idx.jobs.<name> --help
pytest tests/ -q
```

Full setup from scratch, including first-run migrations and seeding: see
README.md "Local dev setup".

## The 5 non-negotiable principles (spec §0)

These exist because violating them silently produces a model that
backtests beautifully and loses money live.

1. **Point-in-time correctness.** Every row carries both `valid_from`
   (when the fact was true) and `created_at`/`ingested_at` (when we
   learned it). Feature-building code may only read rows where
   `created_at <= as_of`. No exceptions.
2. **Append, never overwrite.** Revisions are new rows with a later
   `ingested_at`; the latest wins in views, history stays for audit.
3. **Store raw and adjusted side by side.** Raw for microstructure work,
   adjusted for return series.
4. **Universe is a time series, not a list.** Delisted tickers stay in
   the DB with `delisting_date` set. A backtest that only sees survivors
   is worthless.
5. **Idempotency.** Any job can rerun for any date range without
   corrupting state.

## Conventions

- **`prices_daily_latest` is the default read path.** Never query
  `prices_daily` directly for "what's the current value" — that base
  table holds full revision history (principle #2), and a naive read of
  it can double-count a (ticker, date, source) with more than one
  recorded version. For point-in-time (as-of) reads, use
  `db/queries.py::price_as_of` — the `_latest` view always returns the
  newest version regardless of `as_of`, which is the wrong tool for
  anything that must respect what was knowable at a past moment.
- **`created_at`/`ingested_at` vs `valid_from`/`heard_at`**: the former
  is when we recorded a fact, the latter is when the fact became true
  (or reached us). Never the same field, never conflated. Backdating
  (`created_at < valid_from`) is an integrity violation
  `jobs/validate.py::check_backdating` checks for.
- **Every `prices_daily` write goes through
  `db/upserts.py::upsert_price_bar`.** It writes a new row only on a
  genuinely new-or-changed value — never `session.add(PriceDaily(...))`
  directly outside tests. Skipping this is what would silently multiply
  the table on every rerun (the widened PK no longer rejects a same-key
  re-insert the way the old one did).
- **One commit per validated phase, not per file.** A commit is a
  checkpoint you can actually roll back to — validate before committing,
  not after.
- **Idempotent and resumable by construction, not by convention.** Every
  job should be safe to kill and rerun. Checkpointing (`bootstrap.py`),
  resumable-skip (`harvest_universe_history.py`), insert-only-on-change
  (`upsert_price_bar`) are the established patterns — match them, don't
  reinvent per job.
- **Known issues are for permanent, understood defects — not temporary
  states.** A trading suspension is not a `known_issues` row; it ends,
  and permanent suppression would hide a real re-listing (see
  HANDOFF.md's open thread on this).

## Standing instructions

- **Ask before deviating from spec.** The spec is the contract; Caroline
  is owner/validator. A genuine contradiction (e.g. the `prices_daily`
  PK vs its own revision-behavior wording) gets surfaced and decided
  explicitly, never silently resolved either way.
- **Flag what you can't verify rather than assuming.** IDX's undocumented
  API, tick-size rules, and historical facts about specific tickers are
  all things worth checking empirically before asserting.
- **Prefer loud failure over silent success.** A widened PK misused
  produces duplicate rows — loud, catchable with a one-line assertion. A
  silently-wrong value is the failure mode everything here is built to
  avoid.
