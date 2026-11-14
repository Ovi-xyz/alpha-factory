# 2026-11-13 — SIL-BRZ-SCHEMA-02 on live data, and the Silver 1M duplicate bar (RISK-48 update, RISK-49, FIX SIL-DEDUP-PERIOD-01)

**Input.** Ovi applied `sil_brz_schema_02_code.patch` and `sil_brz_schema_02_docs.patch` and supplied the post-patch log. Then: inspect what the log cannot tell, fix the duplicate November
bar, revise the follow-up patch. Built on GitHub `main` (`fae5154`) plus the two applied patches; nothing was written to the live tree.

## What the log shows
pytest 3611 passed; `silver_ohlcv_context` resumed 1 symbol per timeframe and finished `{'done': 174}`; VIX Silver 1D/1W/1M = 2,565 / 793 / 183 rows; audit: Bronze without Silver 0;
`silver_validate` 15/15 with `context_outlier_detection` at 58 symbols.

## What the log cannot tell, and what reading the files showed
| Question | Answer |
|---|---|
| Which Bronze file was the New York one, and what did its stamps mean? | One file per timeframe, the 5 Nov ingest; wall-clock midnight relabelled New York (0 of 7 equal instants, 7 of 7 equal wall clocks and closes). The by-date rule was right; a conversion would shift every bar a day back |
| Is Silver VIX correct? | 1D: unique stamps, all 00:00 Chicago, 2,514 of 2,514 bars equal to the 11 Nov Bronze file. 1M: two November bars (11-01 and 11-03) |
| Why? | The 3 Nov Bronze 1M file is a one-row partial-window bar stamped 11-03 with 3 Nov values only; other days stamp the month at 11-01 with the month-to-date open |
| How wide? | Gold 1M census: 156 of 200 symbols, November 2026 only (149 with 11-01 + 11-03; 7 with 5-8 fragments and no 11-01 bar; 44 clean). Gold 1W: ABBV only |
| Still growing? | For the 7: yes (Bronze YM_F: full-history file 7 Nov, then small daily files; the 9 Nov file read is a one-day bar stamped 11-09). For the other 149: no |
| Does Silver need a version bump? | No: `silver_ohlcv` re-reads Bronze and rewrites every file per run. The runner CLI cannot clear a Silver checkpoint (`--reset` clears the sentinel only), so a same-day re-run needs the checkpoint cleared in Python (below) |
| Is polars' sort stable enough for "last ingest wins"? | No out-of-order winner at 800,000 rows (polars 1.42.1); not promised by polars, pre-existing, unchanged |
| ABBV weekly Tuesday stamps | A separate defect: 713 of 726 bars Tuesday-stamped, Tuesday bars equal the Friday close in 54 of 425 weeks, Monday bars in 11 of 11. Not fixed |

## Decision: period-keyed dedupe, earliest stamp wins
Considered: latest ingest wins per period (rejected: as of the 3 Nov run the fragment is the latest ingest and would replace the month bar); earliest stamp wins with the latest ingest among equal
stamps (chosen: a partial-window bar is stamped later and covers less; ABBV's Monday bar is the right one); aggregating a month-to-date bar from Silver 1D (not built: would also repair the 7
fragment-only symbols). Week key shifted one day (Sunday..Saturday) so Sunday-stamped forex weeks cannot swallow the previous Monday's bar.

## Verification
3611 -> 3641 collected (+30); 3609 passed, 32 failed (identical to the pristine clone's 32: packages missing in the sandbox venv). 17 mutants killed. New classes:
`TestPeriodDeduplication` (17), `test_audit_silver_period_duplicates.py` (13). Real Silver frames: removes exactly 2026-11-03 (AAPL, SPX, VIX 1M) and 2026-08-18 / 08-25 (ABBV 1W). Two old
fixtures that encoded the bug were corrected on purpose.

## Operator steps after applying the code patch
```
python scripts/audit_silver_period_duplicates.py          # census before: every Silver symbol, 1W and 1M
python -m pytest tests/ -q                                # expect 3641 collected
# the next daily run applies the fix by itself; to apply it today, clear today's Silver checkpoints, then force:
python - <<'EOF'
from datetime import date
from src.utils.progress_checkpoint import ProgressCheckpoint
for job in ("silver_ohlcv_p1", "silver_ohlcv_context"):
    ProgressCheckpoint(job, date.today()).clear(date.today())
EOF
python src/runner.py --job silver_ohlcv --force
python src/runner.py --job silver_ohlcv_context --force
python scripts/audit_silver_period_duplicates.py          # expect exit 0 and "0 symbol(s)"
python src/runner.py --job gold                           # weekly jobs are skipped by the schedule guard
```
Gold check (not scripted): `SELECT symbol, COUNT(*) FROM read_parquet('data/gold/signals/tech_signals_1M.parquet') WHERE CAST(timestamp AS DATE) >= DATE '2026-11-01' GROUP BY 1 HAVING COUNT(*) > 1`
should return the 7 fragment-only symbols at most (one fragment each after the fix: none).

## Mistakes recorded (project mistake log)
Generalising "no new duplicates since 6 Nov" from VIX to every symbol; prescribing a processing_version bump without reading how Silver is rewritten; two fixtures that encoded the bug surfaced
only by running the whole test file.

## Not done
Month-to-date bars for the 7 fragment-only symbols (aggregate Silver 1D); the ABBV Tuesday-binned weekly series; Bronze reads for AG, NKD_F, NQ_F, ES_F; any live Silver/Gold run; the Gatekeeper /
health-report consumption of failed checkpoints (RISK-48 item 4).
