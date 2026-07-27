# 2026-07-27 — Decision C Coverage Tranche (7/7) + pydantic Removal

**Format note:** this is the first entry under the new one-file-per-thread
dev-log convention, replacing the single living
`Alpha_Factory_Development_Log.md`. Each thread gets its own dated file
here (`dev-log/YYYY-MM-DD-short-topic.md`) instead of an in-place rewrite
of one growing document. `CHANGELOG.md` remains the authoritative,
exhaustive per-FIX technical record (root cause / options / fix /
verification) — this file is the narrative companion: what happened this
thread, in what order, and why, for a future thread to orient quickly
without re-deriving it. `KNOWN_RISKS.md` remains the registry for
found-and-fixed or accepted risks (RISK-N).

## Starting state (re-verified empirically, not trusted from any prior document)

- Live `main`: commit `9f7eab3` on `ac3daaa` on `0048382`, `pyproject.toml`
  version `1.11.2` — fresh `git clone`, exact match to every prior
  checkpoint's account. No drift found.
- Uploaded package `alpha-factory-v1_12_0-changed-files.zip` (Decision B
  Steps 2–3 + Decision D, "decided, not implemented, not yet applied to
  live main" per its own `MANIFEST.md`) applied on top of the fresh clone.
  Full independent re-verification against the package's own claims: 1329
  passed, coverage 70.36% exact, 699 instruments, all CI gates (G-1/G-2/
  G-3/G-8) pass — no discrepancies.
- This gave a clean, confirmed v1.12.0 baseline to build on.

## What this thread did

Executed `GMI_Decision_Document_v5.docx` §3 Decision C — the coverage
tranche — which was fully decided and sequenced but not started. All 7
files, in the priority order the decision document itself set (by
downstream consequence, not raw statement count):

1. `gold/mtf_alignment.py` (20%→98%)
2. `gold/screener.py` (31%→**100%**) — "the actual terminal deliverable...
   highest consequence of the 7 if buggy," per Decision C's own framing.
   Turned out to be right: this file had two real, previously-invisible
   bugs.
3. `bronze/fred_ingester.py` (31%→87%)
4. `bronze/bls_ingester.py` (28%→94%)
5. `bronze/imf_ingester.py` (27%→95%)
6. `bronze/eia_ingester.py` (24%→95%) — one more real bug found here.
7. `utils/pipeline_dashboard.py` (29%→99%)

`correlation_matrix.py` / `hmm_regime.py` untouched, per the existing,
already-correct exclusion (confirmed REPLACED by design — Architecture
v2.0 §5.1's own table plus the live `FIX ADR-022/RISK-6` code comment
agree). Still in the coverage denominator, still not exempted from
mattering, just not re-tested against modules their own docstrings say
are superseded.

Net: **1329 → 1469 tests, 70.36% → 81.97% coverage, 0 failed throughout.**

### The pattern that kept repeating: shadow tests vs. real tests

`mtf_alignment.py`'s existing partial test file only ever called a
hand-duplicated copy of its grading/regime logic (`_apply_mock`) — never
the real `_compute_mtf_alignment()`, `_apply_regime_compatible()`, or
`run()`. This is exactly the "shadow-logic tests are worse than no tests"
lesson already logged as a standing principle from earlier v1.12.0-era
work, and it's why coverage sat at 20% despite there being *a* test file.
Writing tests that actually call the real functions against real fixture
Parquet is what surfaced everything below — none of it was found by
reading code, all of it was found by trying to build an honest fixture
and watching something not do what its name says.

### Three real bugs found and fixed (not just documented — see `KNOWN_RISKS.md` RISK-12/13/14 for full writeups)

- **RISK-12 / FIX GLD-SCR-001** — `screener.py`'s regime join used `CROSS
  JOIN` against a subquery that legitimately produces zero rows whenever
  `regime_store.parquet` has no row for the exact `run_date` (missing
  file, a `--force` run ahead of `gold_regime`, or a backfill gap). A
  Cartesian product against an empty relation is empty — this silently
  zeroed the *entire watchlist*, not just the regime columns, regardless
  of how many valid MTF candidates existed. Fixed to `LEFT JOIN ... ON
  TRUE`, matching the graceful-degrade pattern already used a few lines
  above it for `sector_tbl`/`active_tbl`.
- **RISK-13 / FIX GLD-SCR-003** — `_deduplicate_by_cluster()`'s "Max 2 per
  correlation cluster" guard (GD §15.1) has never actually executed for
  any real correlation data: `pl.int_ranges()` (plural — list-producing)
  was used where `pl.int_range()` (singular — per-row scalar) was needed,
  so the rank column came out as `List[Int64]`, and the subsequent
  `filter(cluster_rank < MAX_PER_CLUSTER)` raised a polars `SchemaError`
  every time, silently swallowed by the function's own broad
  `except Exception`. One-character-class fix (drop the "s"), verified
  with a standalone repro before touching the source.
- **RISK-14 / FIX EIA-5** — `eia_ingester.py`'s incremental-fetch cache
  scanned a hardcoded `"data/bronze/commodity/eia/**"` — matching neither
  `self.BASE_PATH` nor the `macro/eia/crude_oil/` domain `write_macro()`
  actually uses. The cache was therefore always empty, and every EIA run
  silently used the full 5-year lookback instead of the intended 14-day
  incremental buffer. Notable because an *earlier*, already-shipped fix
  (`FIX EIA-4`, still in the code as a comment) had correctly repaired how
  the cache is *read* (a key-mismatch bug) without noticing the cache was
  never populated in the first place — a good reminder that a fix can be
  locally correct and still not restore the intended end-to-end behavior.

All three follow the same shape: a broad, individually-reasonable
exception handler existed for a *different* legitimate reason (graceful
degrade on missing optional data), and it also silently absorbed a real
logic bug underneath it. None of these would show up as a crash in
production — only as data quietly not being what it should.

### Hardcode-avoidance, done narrowly

Fixed 3 inline path literals to module-level constants directly in the
files already being touched (`REGIME_STORE_PATH` in `mtf_alignment.py`;
`SILVER_ACTIVE_SYMBOLS_ROOT` / `SILVER_SENTIMENT_ROOT` in `screener.py`) —
needed for test isolation anyway, so low-risk to fix now. **Explicitly not
done:** the same literal path (`data/gold/macro/regime_store.parquet`)
is *also* hardcoded inline in `sector_rotation.py` and `views.py`, and
`pipeline_dashboard.py` has on the order of 15 hardcoded CWD-relative
globs of its own. Both are flagged, not fixed — outside this thread's
file list, and fixing them well would be its own scoped piece of work
rather than a drive-by while doing something else.

### pydantic removed

Re-confirmed a second time (`grep`, zero matches in `src/` or `scripts/`)
that it's genuinely unused, per the explicit gate the prior thread left in
place ("flagged for removal... belongs to whoever confirms there's no
other intended use" — Development Log §9.2 in the old single-file
convention). `poetry remove pydantic`; suite unaffected, as expected.

### Incidental cleanup noticed during packaging

Root-level `CHANGES.diff` and `MANIFEST.md` in the live repo turned out to
be leftover deliverable-package artifacts from the v1.11.1→v1.11.2 thread,
apparently committed along with everything else in that pass rather than
being package-only metadata. Removed as part of this package (they're
genuinely stale — describe a two-versions-ago delta, not this one) rather
than left to accumulate; each new package's own `MANIFEST.md`/
`CHANGES.diff` are meant to travel with the zip, not live in the repo
root. Flagging explicitly here so this isn't a surprise buried in the
diff.

## Verification

- Full suite, working copy: **1469 passed, 0 failed, 0 error.**
- Full suite, independent second fresh clone + fresh venv + fresh
  `poetry install --with dev`: **identical result.**
- Coverage: **81.97%**, Gate G-6 (≥70%) passes with a wide margin.
- Gates re-run manually: G-1 (162 files, `ast.parse` clean), G-2 (0
  f-string SQL), G-3 (`validate_instruments.py` → 699 symbols, exit 0),
  G-8 (0 glob-scope violations). All pass.
- `tests/COUNT_BASELINE.txt` updated 1329 → 1469.
- Version bumped 1.12.0 → 1.12.1 (patch — coverage, bug fixes on already-
  broken paths, and a dependency removal; no interface-contract or
  Silver/Gold schema change).

## What's still open (unchanged from before this thread, except Decision C is now done)

- **GMI Wave 1 Cycle 4 — CrossAssetEngine.** Still the next major
  milestone, still not started, still explicitly out of scope for this
  thread's "before moving into GMI" framing.
- **Ticker/data-source verification** (BIS EER weights, yfinance tickers
  for KRW/SGD/HKD/TWD/NOK and several commodity contexts) — still blocked
  on network access; this sandbox's allowed domains don't include
  yfinance/BIS/Finnhub either, so this remains genuinely unconfirmed, not
  newly resolved.
- **The two flagged-not-fixed hardcode patterns** above
  (`sector_rotation.py`/`views.py`'s inline regime-store path;
  `pipeline_dashboard.py`'s ~15 CWD-relative globs) — reasonable next
  small-scoped pass if wanted, not urgent (both are read paths in
  non-data-correctness-critical code).
- **The `reward_risk_ratio` ATR-invariance observation** in
  `mtf_alignment.py` (see `FIX GLD-MTF-COV-01` in `CHANGELOG.md`) — a
  design question for Ovi, not something this thread decided unilaterally
  since it's outside Decision C's test-only scope.

## Deliverable

`alpha-factory-v1_12_1-changed-files.zip` — `MANIFEST.md` + `CHANGES.diff`
+ all new/modified files, cumulative on top of `9f7eab3` (i.e. includes
everything from the still-unapplied v1.12.0 package too — see this
package's own `MANIFEST.md` for the exact apply order). Not yet applied to
live main — no push access in any session to date.
