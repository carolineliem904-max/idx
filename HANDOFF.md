# HANDOFF.md

Living state doc — current as of 2026-08-08, end of the Phase 2 build
session. Update this when picking work back up or handing off; it's
expected to go stale and get rewritten, unlike CLAUDE.md.

## Phase status

- **Phase 0** — done. Schema + migrations + `AMMN` end-to-end.
- **Phase 1a** — done. 962 active tickers, full Yahoo backfill.
- **Phase 1b** — done, within its documented bound. 27 delisted tickers
  recovered; pre-2020-01-02 delisted tail is a permanent, open gap (IDX's
  own `GetStockSummary` endpoint has no data before that date — verified
  empirically, not a bug).
- **Phase 2** — complete (schema fix + Parts A-G) but **deliberately left
  OPEN**. Spec §7's "10 consecutive green runs" acceptance criterion
  cannot meaningfully accrue on a laptop that sleeps, loses power, or is
  simply off overnight. Closes when this runs on real infrastructure —
  see open thread #5 below.
- **Phase 3+** — not started. See open thread #4 before starting it.

## What's built

Everything in CLAUDE.md's layout section exists and is wired together:
seed → backfill → historical harvest → daily incremental → validate →
reconcile → alert, plus backup/restore/verify and (unloaded) launchd
scheduling. `known_issues` currently holds 49 rows (47 tickers +
2 date-ranges) seeded from confirmed Phase 1 findings.

## What's deliberately NOT built

- **Telegram (or any non-console) notifier.** `Notifier` ABC exists,
  `ConsoleNotifier` is the only implementation. Adding one is a new class
  + registering it in `notify.py::get_notifier()`'s backend switch —
  intentionally left as the trivial case it was designed to be, not
  started because there's no channel to send to yet.
- **launchd plists are not loaded.** Written, `plutil`-validated, wrapper
  scripts smoke-tested directly — but not copied into
  `~/Library/LaunchAgents/` or `launchctl load`ed. That starts an
  indefinite recurring automation hitting live external services; left
  for Caroline to load when ready, not started silently.
- **`broker_flow_daily` is schema-only, zero rows.** Phase 3 hasn't
  started. See open thread #4 — the sizing estimate currently on record
  (24M rows/yr) is a flagged worst case, not a number to build on yet.
- **Suspension-vs-defect distinction (`ticker_status_daily`).** Designed
  in conversation, not implemented — see open thread #2.

## Open threads, in priority order

### 1. FASW — suspected corporate action, IN PROGRESS
5 discrepancies, one ticker, 5 consecutive days
(2026-07-30..2026-08-05) — Yahoo close constant at 5312.4971, IDX close
constant at a different value 5275.0000. Systematic single-ticker
Yahoo/IDX divergence is the classic corporate-action signature (spec
§2.1: raw prices aren't split/dividend-adjusted; if one source applies an
adjustment the other doesn't know about, they drift apart in a way that
looks like noise but isn't). Task in progress at handoff:
1. Pull the actual paired values per date, determine whether the gap is
   a constant **ratio** (points to a split/reverse-split one source
   applied and the other didn't) or a constant **offset** (points to a
   dividend adjustment leaking into what should be an unadjusted
   `close_raw` — which would be a real bug in how we store raw prices,
   not just a stale quote).
2. Re-run `jobs/reconcile.py`'s logic retroactively across the full
   overlapping Yahoo/IDX history (2020-01-02 → today, ~4.2M rows), not
   just the 7-day rolling window `daily.py` uses — report how many
   ticker-episodes it finds. Whatever it finds is unhandled corporate
   actions currently sitting uncorrected in the training data.
Report on this before touching threads #2 or #3 — explicit instruction.

### 2. Zero-volume check misclassifies suspensions
`ASMI`, `BKDP`, `COAL`, `LCKM`, `RGAS`, `MDIA`: consecutive zero-volume
days while still ranking top-300 by trailing median volume. Almost
certainly trading suspension or full call auction (Papan Pemantauan
Khusus), not bad data. Two compounding problems with the current
handling:
- `jobs/validate.py::check_zero_volume_top300` reports it as a
  data-quality **failure** when it's real market information.
- It self-heals in ~2 weeks as the zeros drag the trailing median down
  below the top-300 cutoff, so the finding silently disappears — nobody
  ever learns what actually happened. Same failure class as the
  known-issues-suppression bug (Phase 2 Part D): a real signal vanishing
  without anyone noticing.

**known_issues is the WRONG tool for this** — it's for permanent,
understood defects (the 2007 dates, structurally-suspended tickers with
near-zero history). A trading suspension is a *temporary* state that
ends; permanent suppression would hide a real re-listing.

Design direction (not yet built): an explicit `ticker_status_daily`
concept — trading / suspended / call_auction — derived initially from a
heuristic (≥3 consecutive zero-volume days as a starting rule), but
**check first whether IDX exposes actual suspension announcements** we
could ingest as ground truth instead of inferring it. If it does, that's
strictly better than the heuristic — flag it, don't just default to the
heuristic without checking. This also matters for Phase 4: suspended bars
need to be excludable from feature building, and "currently suspended" is
itself a feature, not just a data-quality nuisance to filter out.

### 3. Alert / log noise
If the current output went to Telegram it would be unreadable.
- Collapse the 47(+)-row `insufficient_yahoo_history` suppressions to one
  line ("N suppressed (insufficient_yahoo_history)") in both the console
  report and any alert — full detail stays queryable from `known_issues`
  / the DB, doesn't need to be printed every time.
- Alerts should carry counts + at most 3 examples per check, with a
  pointer to where the full report lives (not the full finding list
  inline).
- Silence yfinance's own "possibly delisted" stderr chatter for tickers
  already known-suspended — `jobs/validate.py` already logs a structured
  warning for the same fact immediately after; the yfinance stderr line
  is pure duplication.

### 4. Phase 3 gate — measure before designing
Do not start designing `broker_flow_daily` ingestion, ranking, or
anything else until this runs: fetch **one real day** of IDX broker
summary and measure the actual distinct-broker-codes-per-ticker
distribution (min/median/p90/max, and how many tickers appear at all).
The number currently on record — 24M rows/yr, from 960 tickers × 100
broker codes × 250 days — is an explicitly flagged worst case, not a
mean; only ~650 tickers trade on a typical day (measured), and broker
code counts per ticker are believed heavily skewed (liquid names 80+,
thin names 5-10), so a realistic figure is probably nearer 4M rows/yr,
roughly 6x lower. **The Railway/hosting decision (thread #5) depends on
this being measured, not assumed.**

Also already decided, not to be re-litigated when Phase 3 starts (see
CLAUDE.md-style permanence — these are settled, just not yet built):
`broker_flow_daily` scopes to a liquidity-filtered top-~250-by-median-
value-traded subset, using the same query shape
`jobs/validate.py::check_zero_volume_top300` already implements (there,
ranked by median volume; Phase 3's filter ranks by median value traded —
spec §6's own modeling guardrail, not a new idea).

### 5. Railway vs. managed Postgres — deliberately deferred
Blocked on thread #4. Local Docker Postgres remains the target for
everything until a real sizing number exists for `broker_flow_daily` —
deciding hosting on the current worst-case estimate risks either
over-provisioning now or under-provisioning the moment Phase 3 lands.
Everything routes through `DATABASE_URL`, so this should be a config
change when it happens, not a code change — verify that's still true
before deciding.

## Decisions and why (the part compaction destroys)

- **`prices_daily`'s PK was widened to include `ingested_at`, rejecting a
  companion audit-table design.** The audit-table alternative's failure
  mode is *silent*: a reader that forgets to merge the side table gets
  back one plausible-looking row containing the future-corrected value,
  no error, no duplicate, no signal anything's wrong. The widened-PK
  approach's failure mode is *loud*: get the write path wrong and you get
  duplicate rows per (ticker, date) — trivially catchable with a
  one-line assertion (`tests/test_prices_daily_revisions.py` has one).
  Loud beats silent — this is the same principle behind "prefer loud
  failure over silent success" in CLAUDE.md, applied to a real schema
  decision under real cost (a migration against 4.2M live rows).
- **`daily.py` writes a new `prices_daily` row ONLY on an actual value
  change**, never unconditionally. The rolling 7-day window re-touches
  the same days every run; blind appending would add ~7 days × ~962
  tickers × 2 sources ≈ 13k rows/day that record no new information —
  more than doubling the table yearly to encode nothing, and making a
  "revisions written" count meaningless as a signal instead of a real
  one.
- **2020-01-02 is the honest modeling window, not ~2010 as originally
  hoped.** IDX's `GetStockSummary` endpoint has no data before that date
  — verified empirically (every 2010-2019 date probed returns
  `recordsTotal: 0` on a clean 200 OK), not a bug or a rate limit.
  Consequence: delisted tickers are only discoverable from 2020 onward;
  anything delisted earlier is permanently invisible to this pipeline
  unless a different source is found. Pre-2020 Yahoo history is kept
  locally for exploration but is explicitly excluded from the production
  data scope (`idx.config.PRODUCTION_DATA_CUTOFF`).
- **Suspensions must NOT go into `known_issues`.** That table is for
  permanent, understood defects (spec-external facts that won't change:
  the 2007 Yahoo feed anomaly, a ticker's structurally sparse history).
  A suspension is temporary and ends; suppressing it permanently would
  hide a real re-listing when trading resumes. This is why thread #2
  proposes a separate `ticker_status_daily` concept instead of just
  seeding more `known_issues` rows for the zero-volume tickers.
- **Survivorship bias is documented, not laundered.** `securities` is
  962 active + 27 delisted (Phase 1b), explicitly biased for anything
  delisted before 2020-01-01. The README and this file say so in plain
  terms — the alternative (declaring Phase 1 "done" without the caveat)
  is exactly the kind of silent gap spec principle #4 exists to prevent.

## Gotchas

- **Postgres `ON CONFLICT DO NOTHING` still performs a speculative
  insertion internally, even on a true no-op conflict.** Cost `securities`
  (989 rows) 84MB of dead-tuple bloat from ~1.6M redundant harvester
  calls before it was caught and fixed with a known-tickers cache. Regular
  `VACUUM`/autovacuum reclaims dead-tuple space for *reuse*, it does
  **not** shrink the file — only `VACUUM FULL` does that, and it takes an
  exclusive lock. Watch for this pattern anywhere a "does this already
  exist, insert if not" check runs at high frequency against a
  mostly-already-populated table.
- **Railway cron runs in UTC.** Relevant whenever Railway actually gets
  provisioned (thread #5) — spec's "18:30 WIB (11:30 UTC)" schedule needs
  the UTC figure there, not the WIB one. (launchd, by contrast, uses the
  machine's local system timezone — see the caveat baked into
  `launchd/com.idx.daily.plist` itself.)
- **launchd, not cron, on macOS** — specifically because
  `StartCalendarInterval` runs a missed job when the machine wakes from
  sleep, which plain cron does not. A laptop that's asleep at 18:30 WIB
  every day would silently never run under cron.
