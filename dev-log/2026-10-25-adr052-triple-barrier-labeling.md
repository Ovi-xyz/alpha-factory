# 2026-10-25 — ADR-052 triple-barrier labeling (v1.23.0, GMI-TB-001)

Phase order: adr-050 → 051 → 055 → **052** → 053 → 054. This closes 052; 053 (specialists) is next.

## What shipped

- `src/backtest/triple_barrier.py` — `LookaheadTripleBarrierLabeler`, `BarrierConfig`,
  `compute_barrier_widths[_vec]`, `resolve_barriers_vec`. Offline label construction only; lookahead
  by design; banner at the top of the file and the class name say so.
- `scripts/label_triple_barrier.py` — `--from-mtf` (triples from historical `mtf_alignment` rows),
  `--grid SYMS` / `--grid-all` (unconditional calibration sample), `--inspect N` (chart-review CSV).
  Writes only under `data/backtest/triple_barrier/`.
- Tests: 94 + 12 + 9 new; one existing ADR-055 test, parametrised over `src/` files, now also covers
  the new module (+1 ID). 2595 → 2711 collected.
- No job, no `job_registry` entry, no Gold/Silver/Bronze change, no `PITDataLoader` change, no Interface
  Contract change.

## Explore (live data before design)

Two real Silver 1D files were read before any code was written (AAPL, BBCA):

1. Timestamps are tz-aware local-exchange midnight; the trading date is the stamp's own date
   (`.dt.date()`), never a zone conversion — the TS-DATE-01 class. Unit tests run five zones, including
   those east of UTC where a UTC conversion shifts every date back a day.
2. `is_clean=False` is not "corrupt": `_flag_is_clean` also marks |z|>4 moves (BBCA's 2026-10-16 bar is a
   −14.8% day, flagged `is_clean=False`). Filtering the scan on it would drop the bars most likely to touch a barrier. The labeler
   judges bars by structure instead and reports `n_unclean_in_horizon`.
3. `mtf_alignment.date` is the pipeline RUN date, not a bar date. This is the reason for Amendment A1.
4. Median daily true range ≈ 185 bps (same window, AAPL p10/p90 ≈ 113/351). The ADR's 50–100 bps stop
   band is at or below one day's range.

## Decisions (each recorded in code, `label_spec`, CHANGELOG and RISK-39)

- **A1 — key on `decision_date`.** `PITDataLoader` shows bars `< T`; Gold rows are stamped with run date T.
  The ADR's `timestamp > entry_date` only lines up if `entry_date` is the anchor bar, so the labeler derives
  the anchor itself (last bar `< T`) and scans `> anchor`. A literal `> run_date` scan would skip bar T.
- **Joint construction.** `s = clip(k_s·vol, 50, 100)`; `R = clip(k_p·vol/s, 2, 3)`; `p = R·s`. The profit
  band follows from the other two, so it is never filtered afterwards; `BarrierConfig` raises on band
  combinations that could not guarantee it.
- **Open ADR choices made explicit:** time barrier → `loss` (`exclude` available); entry → `next_open`
  (`signal_close` available — what `BacktestEngine` does today); same-bar double touch → `stop_first`;
  horizon 2–7, default 7; gap-through fills at the open.
- **Defaults 0.375 / 0.9375.** First draft was 0.5 / 1.25; on the two live symbols that put the stop on the
  100 bps ceiling 53.0% of days. Compared on identical data: 0.5/1.25 → floor 0.1 / free 46.9 / ceiling
  53.0 %; 0.375/0.9375 → 4.2 / 78.3 / 17.5 %; 0.30/0.75 → 20.9 / 73.9 / 5.2 %. Two symbols only — RISK-39 #1.

## Measured at the shipped default (AAPL + BBCA, 2023-10 → 2026-10, 2,954 labels, both directions)

Base rate 27.5% (zero-drift reference 28.2%); outcomes profit 27.5 / stop 72.0 / time 0.6 %; mean 1.68 bars
held; same-bar ambiguity 2.4%; gap-through 9.2%; R:R clamp: min 0.5 / free 93.1 / max 6.4 %. Long 29.2% vs
short 25.7% pooled (AAPL 31.4% vs 20.7%). 30 of 2,984 inputs skipped (24 `horizon_incomplete`, 6
`anchor_stale`). An uninformed entry wins ~27%; ADR-054's ≥ 0.51 gate is a precision test on fired trades.

## Verification

- Sandbox cloned from GitHub `main` (v1.22.0, 2595 collected, identical to live), dependencies re-pinned to
  `poetry.lock` (duckdb 1.5.4, polars 1.42.1). The first sandbox had duckdb/polars 2.0.0, where
  `DuckDBPyConnection.pl()` is absent — a version artefact, not a repo bug; the locked versions are what
  count.
- Full suite: 2709 passed, 2 failed. Both are `tests/unit/test_check_poetry_env.py` (no `poetry` binary
  in the sandbox); identical on pristine `main` (`git stash` run).
- Reference check: a loop-based resolver in the tests (independent logic) vs the vectorised resolver on
  3 policies × 2 directions × 600 random windows with gaps and fat ranges; asserts the interesting
  branches occurred.
- Mutants: 18 on the labeler, 4 on the isolation pin — all killed. Two labeler mutants first SURVIVED:
  (a) removing gap flags from the hit masks — the test bars let another path register the touch, so a
  malformed bar (open beyond a barrier, range disagreeing) was added; (b) removing the R:R ceiling — masked
  by the final profit-band clip under the default multiplier ratio, so a test with a ratio outside [2, 3]
  was added. A third gap (bar repair unobservable through touch logic) was found the same way and is
  tested through the vol estimate.
- Not done: the manual chart-vs-label inspection ADR-052 requires, and the full-universe `--grid-all`
  audit. Both need the live Silver store and are operator steps.

## Operator runbook (after the live mirror)

```
python -m pytest tests/ -q                                       # expect 2711 collected
python scripts/label_triple_barrier.py --grid-all --years 3 --step 5 --inspect 25
#   review: per-market stop_at_floor / stop_at_ceiling, win_rate vs zero_drift_ref, same_bar_amb
#   open data/backtest/triple_barrier/inspection_<tag>.csv and compare ~25 trades with raw charts
python scripts/label_triple_barrier.py --from-mtf                # production-shaped sample (history-limited)
```
