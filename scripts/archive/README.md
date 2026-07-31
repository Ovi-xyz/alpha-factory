# scripts/archive/

Retired code kept for **historical reference only**. Two distinct
categories live here, and they are NOT the same kind of "archived":

1. **Destructive migration scripts** (`migrate_instruments.py`,
   `build_instruments_v14.py`, `instruments_raw.py`) — disabled with a
   hard `raise SystemExit(...)` guard at the top of the file because they
   write to `config/instruments.yaml` at *module import time*, not just on
   direct execution. See "Why archived" below.
2. **Retired tvdatafeed modules** (`tvdatafeed_adapter.py`,
   `tvdatafeed_session.py`, `check_tvdatafeed_symbols.py`, and the two
   `ARCHIVED_test_*.py` files) — plain library code and tests with no
   import-time side effects. No `SystemExit` guard needed or added; a
   plain move sufficed. See "tvdatafeed retirement (ADR-029)" below.

## tvdatafeed retirement (ADR-029)

5 files moved here 30 Jul 2026 per `GMI_Decision_Document_v7.docx` Decision
I (ADR-029) — tvdatafeed retired entirely as a platform dependency.
tvdatafeed's sign-in had been failing since >=29 Jul 2026 (nologin fallback
mode; every non-IDX exchange fetch timed out even on a nominally "healthy"
session — see `alpha-factory_preflight_logs___29_July_2026.txt`). yfinance
`.JK` was already the tested `ChainedAdapter` fallback for IDX30 and is now
its sole source. See `KNOWN_RISKS.md` RISK-1 (RESOLVED).

| File | Original location | Notes |
| --- | --- | --- |
| `tvdatafeed_adapter.py` | `src/bronze/` | `TvDatafeedAdapter` (`SourceAdapter` impl). No import-time side effects — plain move. |
| `tvdatafeed_session.py` | `src/bronze/` | `TvDatafeedSessionManager` singleton, session/reconnect logic for the adapter above. |
| `check_tvdatafeed_symbols.py` | `scripts/preflight/` | ADR-025/GMI v6 preflight script (OD-C1 routing table). Superseded, not resolved-by-verification — CPO/RUBBER/TIN/NICKEL now route to yfinance equity proxies (ADR-030–033) instead. |
| `ARCHIVED_test_tvdatafeed_adapter.py` | `tests/unit/test_tvdatafeed_adapter.py` | Renamed (dropped `test_` prefix) so it is never collected by pytest — it imports the now-archived adapter above and would error on collection otherwise. |
| `ARCHIVED_test_tvdatafeed_session.py` | `tests/unit/test_tvdatafeed_session.py` | Same rename rationale. |

Neither `TvDatafeed*` class ever had a destructive write path (they only
*read* from an external API), so unlike the migration scripts below, no
`SystemExit` guard was needed — GMI v7's own framing was "a plain move
suffices." `market_ingester.py`'s `TvDatafeedAdapter` import was removed;
`_primary_source_for()` and the `idx_chain` construction were updated to
yfinance-only. If tvdatafeed's sign-in issue is ever resolved upstream and
re-adoption is wanted, treat these as a starting reference only —
re-verify against the then-current `SourceAdapter`/`ChainedAdapter`
interfaces rather than restoring them verbatim.

## Why archived (migration scripts)

| Script | Wrote to | Reads from | Why it's now dangerous |
| --- | --- | --- | --- |
| `migrate_instruments.py` | `config/instruments.yaml` | `src/config/instruments_raw.py` | Source is the original Grand Design v1.2 flat structure (643 instruments, 4 markets, no Layer 2). Running this against the current pipeline would overwrite the hand-maintained v1.5 instruments.yaml with that stale 4-version-old shape. |
| `build_instruments_v14.py` | `config/instruments.yaml` | `config/instruments.yaml` (**same path**) | One-time in-place v1.2→v1.4 transform. `SRC == DST`. instruments.yaml is now at v1.5 — re-running would read the *current* file and overwrite it with v1.4-era output, silently discarding everything added since (commodity taxonomy, domain-score `_meta.contributes_to` routing, 79+ hand-written ADR-rationale comments). No external input to diff against, no backup step — the more dangerous of the two. |

Both scripts have **no `if __name__ == "__main__":` guard** — the destructive
write executed at *module import time*, not just on direct execution. This
was found empirically (running each one, then trying `import
scripts.archive.migrate_instruments`) while assessing them for archival, not
by reading their docstrings, which never mentioned it.

## Third file: `instruments_raw.py`

Relocated here from `src/config/instruments_raw.py` in the same pass. It is
pure constant data (`US_STOCKS_BY_SECTOR`, `IDX_STOCKS`, `COMMODITY`,
`FOREX` dict/list literals, zero functions or classes) with exactly one
consumer in the entire codebase: `migrate_instruments.py`, above — which is
itself archived and guarded. Leaving 700 lines of dead data sitting in
`src/config/` implied it was live production config; it also permanently
diluted the `src/` coverage denominator for a file that, by design, will
never be exercised again. Moving it here removes it from
`[tool.coverage.run] source = ["src"]` scope entirely — no `omit` entry
needed, and coverage for `src/` now genuinely reflects live code.

## Discovered / archived

v1.11.2, per `GMI_Decision_Document_v3.docx` Priority 3 (also carried in
`GMI_Implementation_Checkpoint_v6.docx` §8 item 3: *"lowest-ambiguity item,
ready to implement directly"*). Neither script had been referenced by CI,
and `migrate_instruments.py` was only reachable via `make migrate` (also
updated this pass — see repo root `Makefile`, target now fails loudly
instead of running).

## If a genuine full rebuild is ever needed

Do **not** just delete the `raise SystemExit(...)` guard and run either
script as-is — both target a schema several versions behind current. A real
rebuild would mean:

1. Writing a fresh migration against the *current* `config/instruments.yaml`
   v1.5 schema (see `GMI_Decision_Document_v3.docx` §3 / `GMI_Decision_Document_v4.docx`
   Priority 1 — the still-open instruments.yaml split work is the more
   relevant starting point than resurrecting either archived script).
2. Running it against a copy, diffing the output against the live file
   field-by-field, and getting explicit sign-off before touching the real
   `config/instruments.yaml` — per this project's own "jangan gegabah"
   discipline (explore → decide → implement, never collapsed).
