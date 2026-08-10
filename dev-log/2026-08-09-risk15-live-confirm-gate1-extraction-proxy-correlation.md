# 2026-08-09 — RISK-15 Live-Confirm, Gate 1 Extraction Pass, Proxy Correlation Studies

**Version**: 1.14.0 → 1.15.0
**Trigger**: Ovi provided a freshly-run `check_fred_commodity_series.py` M1 preflight log ("preflight logs stored in context"), then directed work to start on two "on the horizon" items: Gate 1 BIS weight extraction, and proxy correlation studies for F34.SI/STA.BK/AFM.V/NIC.AX.
**Scope**: `scripts/preflight/`, `tests/unit/test_preflight_scripts.py`, `tests/COUNT_BASELINE.txt`, `KNOWN_RISKS.md`, `CHANGELOG.md`, `pyproject.toml`. **No `src/` change.**

---

## 0. Exploration before any code was written

Before touching anything, read directly from the live repo (Filesystem MCP) to build precise ground truth rather than trust prior-session memory as-is:

- The 6 most recent dev-logs (2026-08-01 through 2026-08-08) — revealed Gate 1 detail far more precise than remembered: `weightsb.xlsx`'s layout is fully confirmed (row 6 = header, column 2 = row-label, identical across all 10 sheets), but the US row itself was **never searched for**, since "US" is not one of the 13 target REF_AREA codes that were scanned.
- `pyproject.toml`, `tests/COUNT_BASELINE.txt` — actual version is 1.14.0 / baseline 1432 (not 1.13.5/1422 as previously remembered) — RISK-15 had in fact already been fully closed out in a prior session, dev-log and CHANGELOG included.
- `KNOWN_RISKS.md` RISK-15 (grepped and viewed directly, not assumed): the entry itself explicitly left open "Not yet run: a real `FRED_API_KEY`-backed invocation ... against live FRED, and a full `poetry run pytest`" — exactly the two things still unverified. The log Ovi provided today closes **half** of that note (the live FRED check), not the whole thing (the full pytest run was not included).
- `KNOWN_RISKS.md` Gate 1 (RISK-16 section): "**still not closed** — the scan gives coordinates, not values... The targeted extraction pass — find the US row, read its 13 target-column values — is unblocked but not started."
- `config/instruments_taxonomy.yaml` (grepped for CPO/RUBBER/TIN/NICKEL): all four entries explicitly note inline that `proxy_for`/`proxy_correlation_expected` are **deliberately unset** — "no empirical [ticker]-vs-[commodity]-price correlation analysis exists yet (unlike VALE's ~0.81, ADR-005)."
- `scripts/preflight/check_bis_eer_weights.py` (full read) — to understand the EXISTING indexing convention (1-indexed row/col, the exact same scan pattern `_discover_weights()` already uses) before writing any extension.
- `scripts/preflight/check_yfinance_tickers.py`, `check_fred_commodity_series.py` (full read) — to match the I/O + testing pattern the new correlation script needed to follow precisely.
- `tests/unit/test_preflight_scripts.py` (full read) — class-per-script structure, mocking conventions, docstring style.

Key takeaway from this exploration: **RISK-15 was already fully documented** in a prior session (2026-08-08 dev-log complete, CHANGELOG v1.14.0 complete) — the earlier memory claim that "dev-log and CHANGELOG haven't been written yet" was stale. What actually remained was live verification, not documentation written from scratch.

## 1. RISK-15 — Live-Confirm (not a rewrite)

Log Ovi provided (`check_fred_commodity_series.py`, run on the M1, real `FRED_API_KEY`):

```
[PASS] PCOALAUUSDM  latest=2026-06-01, 12/12 usable
[PASS] PIORECRUSDM  latest=2026-06-01, 12/12 usable
[PASS] PNICKUSDM    latest=2026-06-01, 12/12 usable
[PASS] PPOILUSDM    latest=2026-06-01, 12/12 usable
[PASS] PRUBBUSDM    latest=2026-06-01, 12/12 usable
[PASS] PTINUSDM     latest=2026-06-01, 12/12 usable
```

`KNOWN_RISKS.md` RISK-15 updated: the "Not yet run" paragraph split into "Live-confirmed (9 Aug 2026)" for the FRED half, and "Still not run" for the `poetry run pytest` half, which stays open — **not claimed done**, since that log wasn't part of this session. No code/config change here — this part is purely a risk-registry status update.

## 2. Gate 1 — `extract_us_weights_from_sheet()` + `--extract-weights`

**Precise root cause** (from `KNOWN_RISKS.md`'s own Gate 1 section, not a guess): the 4 Aug `--discover-weights` scan searched for the 13 target codes as both rows and columns, but never searched for a "US" row itself — because "US" isn't a member of `BROAD_DOLLAR_REF_AREAS`. Not a bug; a precisely-documented design gap.

**Implementation**: `extract_us_weights_from_sheet(ws, ref_areas, us_ref_area="US", max_scan_rows=200)` — pure function:
1. Single scan (≤200 rows) for the row whose column-2 cell equals `us_ref_area`.
2. Independent scan for the header row (≥2 of the 13 target codes as cell values — not hardcoded to row 6, even though the 4 Aug run found identical positions across all 10 sheets; positions are still re-derived on every call).
3. Read the intersection: the US row's value at each target currency's column.
4. Returns `None` if no US row is found at all; returns a dict with per-currency `None` (not a total failure) if some columns aren't found.

`_extract_weights(sheet=None, us_ref_area="US")` — the I/O wrapper: download, open the workbook, select a sheet (defaults to `max(sheetnames)` → `2020_2022`, overridable with `--sheet`), call the pure function above, print a per-currency report.

**A mistake found and fixed mid-process (documented, not hidden)**: the first smoke test for the "missing currency column" case (Test 3) failed — the assertion checked for `1.0`/`2.0`, which turned out to be the Australia row's own values (built in the same test), not the US row's values that should actually have been checked (`5.5`/`6.6`). The extraction function was correct; the test's anchor was wrong. Traced manually before fixing, not just patched until green — noted in the test's own comments as an audit trail. A second case: an early end-to-end smoke test for `_extract_weights()` used only 1 target currency (AUD) and failed, because the header-row heuristic (`_HEADER_ROW_MIN_HITS = 2`) needs at least 2 matching codes to reliably tell a header row apart from an ordinary data row — not a bug, but a real, now-documented limitation in the function's docstring (irrelevant to actual usage, which always passes all 13 currencies).

**Verification**: 11 new tests (`TestCheckBisEerWeights::test_extract_*`) — happy path with all 13 currencies, missing US row, missing currency column (partial result), `us_ref_area` override, auto-selecting the most recent vintage vs. an explicit `--sheet` override, unknown sheet, US row not found, CLI wiring. All against a synthetic workbook shaped like the real, already-confirmed layout — not the real `bis.org` file, since no sandbox on this project has a network route there.

**Gate 1's status now**: extraction code written and fully unit-tested. **Still not closed** — has never been run against the real `weightsb.xlsx`. Next step: `python scripts/preflight/check_bis_eer_weights.py --extract-weights` on the M1.

## 3. Proxy Correlation Studies — `check_proxy_correlation.py` (new script)

**Why now**: RISK-15 itself (v1.14.0) already noted this correlation study was waiting on the FRED Track 2 benchmark — confirmed live in section 1 above.

**Methodology, and why it differs from VALE (~0.78-0.85, ADR-005)**: no official daily benchmark exists for CPO/RUBBER/TIN/NICKEL — that's precisely why they're proxied at all (the raw BMDI/SGX/LME feeds ADR-029 retired). The only empirical benchmark available is the **monthly** FRED Track 2 series. So: the yfinance proxy (daily) is resampled to a monthly close (the 1st of each month, matching FRED's own date convention), correlated as a **month-over-month return** (not price level — avoiding spurious correlation between two independently trending series, the same principle CorrelationModule already applies platform-wide, Architecture v2.0 §6.2) against FRED's monthly return. The methodological mismatch (point-in-time proxy price vs. FRED's month-average) is stated explicitly in the docstring, not hidden.

`compute_proxy_correlation()` — a pure function (stdlib `statistics.correlation`, no numpy/scipy) separated from I/O, same pattern as `extract_us_weights_from_sheet()`. A minimum of 12 overlapping return pairs is required — fails closed (returns `None`) rather than producing a possibly-misleading number from too few data points.

**Verified**: 17 new tests (`TestCheckProxyCorrelation`) — pure correlation math (perfect positive, perfect negative, insufficient overlap, zero variance without crashing), I/O wrapper error handling (yfinance/FRED exceptions, empty data), the success message citing the VALE/ADR-005 reference point rather than an invented pass/fail threshold, CLI wiring. Cross-consistency guard: `BENCHMARK_SERIES` is validated as a subset of `check_fred_commodity_series.py`'s `EXPECTED_COMMODITY_SERIES`.

**Not yet run** against real yfinance/FRED. Filling in `proxy_for`/`proxy_correlation_expected` in `instruments_taxonomy.yaml` is a deliberate follow-up decision this script does not make — same as IRON_ORE/COAL_NEWC, the number has to exist before the config is written.

## 4. Full verification before anything touched the real repo

The same sequence held every prior session:

1. Both scripts written in the sandbox, `ast.parse()` — OK.
2. Manual smoke tests per pure function (see the mistakes found and fixed above).
3. Full pytest tests written, merged into a FULL RECONSTRUCTION of `test_preflight_scripts.py` (the other 5 preflight scripts copied verbatim from the live repo — not stubbed) in a separate isolated sandbox (`repo_test/`).
4. `pytest --collect-only` — 71 tests collected cleanly, no collection errors.
5. `pytest -q` — 1 failure found (`TestCheckYfinanceTickers::test_main_returns_1_for_unknown_symbol_filter`), traced: purely an artifact of an overly-simple sandbox `InstrumentLoader` stub (raised `NotImplementedError` instead of behaving like the real one), **not** related to this session's changes — `check_yfinance_tickers.py` and its test class were never touched. Stub improved just enough (returns a fake instrument list instead of raising) — 71/71 pass.
6. Both scripts + the test file written to the live repo (Filesystem MCP `write_file`).
7. **Closed-loop read-back**: all three files re-fetched from the live repo and diffed byte-for-byte against the sandbox versions — **identical**, no exceptions.
8. As a final check: pytest run one more time against the copy just re-fetched from the live repo (not the sandbox draft) — 71/71 pass, locking in that what's actually written to the repo works, not just the draft.
9. `ast.parse()` + an f-string SQL anti-pattern grep (this project's own G-1/G-2 CI gates) run once more against the newly-written files — clean (not really applicable here, neither script touches SQL, but run anyway for consistency).

## 5. Files changed

| File | Change |
| --- | --- |
| `scripts/preflight/check_bis_eer_weights.py` | UPDATE: +`extract_us_weights_from_sheet()`, +`_extract_weights()`, +`--extract-weights`/`--sheet`/`--us-ref-area` CLI |
| `scripts/preflight/check_proxy_correlation.py` | NEW |
| `tests/unit/test_preflight_scripts.py` | UPDATE: +11 tests (Gate 1) +17 tests (`TestCheckProxyCorrelation`) = +28 |
| `tests/COUNT_BASELINE.txt` | 1432 → 1460 |
| `KNOWN_RISKS.md` | RISK-15 live-confirm paragraph; RISK-1 proxy-correlation note; Gate 1/RISK-16 extraction-pass paragraph; "Last updated" footer |
| `CHANGELOG.md` | New v1.15.0 entry (3 sub-sections: VERIFY, ADD ×2) |
| `pyproject.toml` | version 1.14.0 → 1.15.0 |

**Not changed**: anything under `src/`. MINOR bump (not PATCH) because two new preflight capabilities were added — following the same MINOR precedent as v1.14.0 (RISK-15), not a pure bug fix.

## 6. What's still open (not claimed done)

- **RISK-15**: `poetry run pytest` on real hardware confirming 1432/1432 (now stale — the baseline moved to 1460 this session) — never run.
- **Gate 1**: `--extract-weights` has never been run against the real `weightsb.xlsx`. Its output is just 13 numbers — no decision yet on how to wire them into `BIS_WEIGHTS`/`broad_dollar.py` (that's a separate design step, not part of this extraction pass).
- **Proxy correlation studies**: `check_proxy_correlation.py` has never been run against real yfinance/FRED. Once it is: deciding `proxy_for` (a benchmark identifier, not a ticker — the `IRON_ORE_SGX_FE62` pattern) and `proxy_correlation_expected` in `instruments_taxonomy.yaml` for all four instruments is a separate follow-up step, deliberately not automated by this script.

**Next on the M1** (two commands, independent of each other):

```bash
python scripts/preflight/check_bis_eer_weights.py --extract-weights
python scripts/preflight/check_proxy_correlation.py
```
