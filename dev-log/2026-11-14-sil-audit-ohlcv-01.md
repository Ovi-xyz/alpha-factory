# 2026-11-14 — A thorough, read-only Silver OHLCV audit (FIX SIL-AUDIT-OHLCV-01, RISK-50)

**Input.** Ovi asked for an update patch containing a script that audits Silver OHLCV thoroughly, based on two open findings of FIX SIL-DEDUP-PERIOD-01: seven 1M symbols (YM_F, NKD_F, NQ_F, ES_F, AU, AG,
CL) whose Bronze holds only fragments and that have no 2026-11-01 bar, and ABBV's Tuesday-stamped 1W series. Built on GitHub `main` (`a67007b`), which matched the live tree for the processor, the CHANGELOG head,
the KNOWN_RISKS tail and `COUNT_BASELINE`; nothing was written to the live tree.

## What reading the real files changed
The first draft judged a weekly/monthly bar on min(open, high, low, close) at tol 5e-4 / min-match 0.98 and compared the current period's open with the first daily open. Run on five real series it would
have raised WARNs on every dividend payer and every FX pair. What the data said, and what the script does now:
| Observation (14 Nov 2026) | Consequence in the script |
|---|---|
| AAPL 1W: close equals 1D close to 6e-7 on 531 weeks, open misses 33 (4 a year around Feb/May/Aug/Nov) | judge CLOSE only; report O/H/L/volume; no open comparison in `current_period` |
| ABBV: Tuesday bars equal Tue..Mon blocks (close 518/521) and fail Mon..Fri (71/521) | two reconciliations (stamp window = values, calendar window = convention); the disagreement is the diagnosis |
| YM_F 1M: 27 of 180 months have no bar; a gap made the previous bar's stamp-to-stamp window two months long | window end capped at one nominal period; new `gaps` check |
| EUR_USD 1W: close = the NEXT Monday's daily close in 2016-17, 0.37-0.79 per year afterwards | end-inclusive variant recognised and reported; the mismatch itself is left as a WARN |
| `is_clean` null on row 0 of 10 of 10 files (`True & null`) | INFO for the first bar, FAIL only for nulls elsewhere (a FAIL would be a permanent red) |
| Calibration at tol 1e-3: healthy >= 0.987, defects <= 0.583 | defaults `--tol 1e-3`, `--min-match 0.95`, marked [U] (n = 5) |

## The checks
`schema`, `timestamp`, `period`, `anchor`, `window`, `gaps`, `current_period`, `ohlc`, `derived`, `meta`, `freshness`, `coverage` (`--list-checks`). FAIL = structural invariant (exit 1); WARN = statistical or
convention (exit 1 only with `--strict`); INFO = census. `period_key_expr` and the processor constants are imported, not copied.

## Verification
3641 -> 3747 collected (+106); sandbox 3724 passed / 23 failed, the 23 identical on a pristine clone (missing `fredapi`, `yfinance`, `httpx`, `pandas_ta_classic`). 37/37 mutants killed (one survivor in
round one, closed by a String-timestamp test). Real files: 5 FAIL / 4 WARN, all matching the findings; real registry (654 symbols) loads. Not verified: the full tree, run time, `--tf all` on 1H/4H, 17 FX pairs.

## Operator steps after applying the code patch
```
python -m pytest tests/unit/test_audit_silver_ohlcv.py -q          # expect 106 passed
python -m pytest tests/ -q                                         # expect 3747 collected
python scripts/audit_silver_ohlcv.py --list-checks
python scripts/audit_silver_ohlcv.py                               # whole tree, 1D/1W/1M; exit 1 expected until Silver is rebuilt
python scripts/audit_silver_ohlcv.py --symbol ABBV --symbol YM_F --checks anchor,period,window,gaps,current_period
python scripts/audit_silver_ohlcv.py --format json > silver_audit_2026-11-14.json    # keep for the before/after comparison
# after the RISK-49 fix has been applied and Silver re-run: run it again and diff the two JSON files
```
Time the first whole-tree run and note it; it has not been measured.

## Open
ABBV's Tuesday anchor, the seven fragment-only symbols, YM_F's missing months, EUR_USD 1W vs 1D (and whether the other 17 pairs match), and the null first-bar `is_clean` are measured here and unchanged.
