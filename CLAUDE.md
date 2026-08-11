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
  review/                    generated human-review artifacts — NEVER auto-applied to the DB, see HANDOFF.md
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
      validate.py                 data quality gate, known_issues-aware; full-history-audit lives here too
      seed_known_issues.py        seeds known_issues from confirmed findings
      reconcile.py                 cross-source (Yahoo vs IDX) discrepancy detection, writes price_discrepancies
      classify_discrepancies.py    A/B/C/D root-cause classification of what reconcile.py finds — see HANDOFF.md
      generate_ca_review.py        writes review/corporate_actions_candidates.md — review only, never writes to the DB
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
   adjusted for return series. (As of 2026-08-09: "raw" needs one more
   qualifier — see SOURCE AUTHORITY below. Yahoo's `close_raw` turned out
   not to be reliably raw. This principle still holds; which *source's*
   `close_raw` you trust for a given purpose is now more specific.)
4. **Universe is a time series, not a list.** Delisted tickers stay in
   the DB with `delisting_date` set. A backtest that only sees survivors
   is worthless.
5. **Idempotency.** Any job can rerun for any date range without
   corrupting state.

## Conventions

- **SOURCE AUTHORITY (added 2026-08-09):** IDX is authoritative for
  `close_raw`, from 2020-01-02 onward. Yahoo is authoritative for
  `close_adj`. Neither source is "the" source — they are authoritative
  for *different columns*, and any code reading price levels must know
  which one it needs. Level-based work (tick size, ARA/ARB bands, gap
  detection, price-based liquidity filters) reads IDX's `close_raw`.
  Return-series work reads Yahoo's `close_adj`. Full evidence and the
  investigation that produced this in HANDOFF.md — don't re-derive it,
  it's already been proven three separate ways.
- **AT_FLOOR is a real market state, not a data defect (added
  2026-08-11).** Rp50 is IDX's minimum tick price ("gocap" in local
  trading slang) — a hard, exchange-enforced floor. A ticker can trade
  there for months or years, with real (sometimes huge) volume, simply
  because the exchange mechanically won't let the price go lower.
  Confirmed via BNBR: 82% of 2020-01-02→2023-02-15 sat at exactly 50,
  with up to 253M shares traded in a single day — that is not a freeze,
  it's the real price. This started as a wrong hypothesis (a suspected
  suspension) that the IDX volume data corrected — don't re-litigate it.
  A naive "close never changes across many days" check cannot tell
  floor-pinning apart from genuine staleness; per SOURCE AUTHORITY
  above, the exception only ever applies to IDX's `close_raw` (Yahoo
  isn't authoritative for it, so a Yahoo close that happens to equal 50
  proves nothing). `jobs/classify_discrepancies.py`'s `IDX_FLOOR_PRICE`
  / `FrozenRun.at_floor` implement this — without it, a ticker
  genuinely fine on the IDX side but broken on the Yahoo side reads as
  "both sides frozen, not a data-quality defect" and the real Yahoo
  defect gets silently buried (found this exact failure on BBRM: IDX
  pinned at 50 for a real 522-day run while Yahoo sat frozen at an
  unrelated 67.7745 the entire time). This is not a rare edge case: 303
  of 989 IDX tickers (31%) have spent at least one real-volume day at
  the floor, and 82 (8.3%) have spent over a quarter of their history
  there — a non-trivial slice of the universe worth remembering for any
  future liquidity filtering or feature work, not just this classifier.
  Full investigation and the universe-wide breakdown in HANDOFF.md.
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
- **`jobs/validate.py full-history-audit` is a standing check, not a
  one-off.** It catches category-B-style single-source price freezes
  (see HANDOFF.md) that the normal rolling-window checks structurally
  cannot see. Run it after any bulk ingest (a fresh backfill, a
  historical harvest re-run) — not just periodically.
- **Generated review files (`review/*.md`) never get applied to the
  database automatically.** Anything under `review/` is a proposal for a
  human to read, tier by tier, and approve explicitly. If you find
  yourself writing code that reads a `review/` file and writes to the DB
  without a human step in between, stop — that defeats the entire point
  of the tier structure.
- **One commit per validated phase, not per file.** A commit is a
  checkpoint you can actually roll back to — validate before committing,
  not after.
- **Commit directly to `main`; no feature branches (decided
  2026-08-11).** Solo, single-machine project, and validation already
  happens before each commit (see above) — a branch would add process
  without adding safety here, and a branch that sits unmerged is exactly
  how `main` ends up carrying stale docs while the real state lives
  elsewhere (hit this literally the session before this decision was
  made). Same granularity as above: one commit per validated unit of
  work, just landed straight on `main` instead of a branch-then-PR
  dance. Revisit if this ever becomes multi-contributor or multi-machine
  — the tradeoff changes the moment two people (or two clones) can push
  at once.
- **Idempotent and resumable by construction, not by convention.** Every
  job should be safe to kill and rerun. Checkpointing (`bootstrap.py`),
  resumable-skip (`harvest_universe_history.py`), insert-only-on-change
  (`upsert_price_bar`) are the established patterns — match them, don't
  reinvent per job.
- **Known issues are for permanent, understood defects — not temporary
  states.** A trading suspension is not a `known_issues` row; it ends,
  and permanent suppression would hide a real re-listing (see
  HANDOFF.md's open threads).

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
- **SILENT SUCCESS IS THE DOMINANT FAILURE MODE IN THIS PROJECT.** Not a
  category among others — the default suspicion. Five confirmed
  instances so far, each one a check or a design that LOOKED clean and
  wasn't:
  1. `ON CONFLICT DO NOTHING` still writes a dead tuple even on a true
     no-op, silently bloating `securities` to 84MB with zero errors
     anywhere.
  2. A SQLAlchemy column default silently set every newly-discovered
     ticker `is_active=True`, making the delisted-ticker diff empty by
     construction — a real bug that produced a *plausible*, not
     obviously-wrong, zero-count result.
  3. The rejected companion-audit-table design for point-in-time reads:
     forgetting to merge the side table would have served a
     future-corrected value to an as-of read with no error, no
     duplicate, no signal anything was wrong.
  4. The A/B/C/D freeze detector's own noise floor misfiled ADMF's clean
     289-day Yahoo freeze as "both frozen" (category C) — a two-day
     coincidental repeat on the *moving* side was enough to hide a real
     289-day defect on the *frozen* side. Took a dedicated regression
     test to lock the fix in.
  5. That same detector, before the AT_FLOOR fix, filed BBRM's real
     522-day Yahoo freeze (stuck at 67.7745) as category C ("both
     frozen, not a data-quality defect") because IDX's side was *also*
     bit-identical the whole time — except IDX was fine, genuinely
     trading at the Rp50 exchange floor. "Both sides look frozen" read
     as a clean, boring non-finding; it was a real 2-year defect wearing
     a suspension costume. 16 tickers, 77 runs were misfiled this way.
  When a check passes cleanly, or a result looks plausible, ask
  explicitly: **can this fail silently, and would I have noticed if it
  had?** If the answer isn't a confident no, that's the next thing to
  verify — not the next thing to trust.
