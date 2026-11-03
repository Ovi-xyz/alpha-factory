"""
scripts/studies/hmm_catchup_audit.py — ADR-058 HMM-R01 step 3: the catch-up audit. OFFLINE UTILITY.

Run it after the classify jobs have scored a gap and BEFORE any fit (the guide's live order, step 3).
Read-only; not a pipeline job; not imported from `src/` (pinned in tests/integration/test_hmm_update_adr058.py).
It imports nothing from `src`: it reads the stores and the persisted model JSON with DuckDB, parameterised
(`$name`), and answers four questions:

    A1  out-of-sample rows already seen by their model (must be 0)  — `NOT in_sample AND model_train_end >= date`
    A2  out-of-sample accounting per store (rows / first / last date per `in_sample` value)
    A3  context rows with BOTH sides out-of-sample, after --since     (the guide expects 10 after its catch-up)
    A4  model fingerprints on rows written after --since, versus the fingerprints on disk

Rows written before ADR-058 HMM-R02 have a NULL `model_train_end`; A1 cannot test them (a NULL comparison is
not true) and the count of out-of-sample rows with a NULL is reported as `unverifiable_legacy_oos` rather than
silently counted as clean.

    python scripts/studies/hmm_catchup_audit.py --since 2026-10-16 [--hmm-dir data/gold/hmm] [--expect-context-rows 10]
Exit status 1 when A1 > 0, a fingerprint mismatches, or --expect-context-rows is not met.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

import duckdb

MACRO_DOMAINS = ("monetary", "growth", "inflation", "stress", "liquidity")


@dataclass
class Audit:
    oos_seen_by_model: dict[str, int] = field(default_factory=dict)       # A1 per store
    accounting: dict[str, list[tuple]] = field(default_factory=dict)      # A2 per store: (in_sample, n, d0, d1)
    unverifiable_legacy_oos: dict[str, int] = field(default_factory=dict)
    context_both_oos_since: int = 0                                       # A3
    fingerprints: dict[str, dict] = field(default_factory=dict)           # A4 per store
    problems: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.problems


def _q(con, sql: str, **params):
    return con.execute(sql, params).fetchall()


def _has_col(con, path: str, col: str) -> bool:
    return col in [r[0] for r in _q(con, "SELECT name FROM parquet_schema($p)", p=path)]


def _fingerprints_on_disk(models_dir: Path) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {"market": set(), "macro": set()}
    m = models_dir / "market.json"
    if m.exists():
        out["market"].add(json.loads(m.read_text())["config_fingerprint"])
    for d in MACRO_DOMAINS:
        f = models_dir / f"macro_{d}.json"
        if f.exists():
            out["macro"].add(json.loads(f.read_text())["config_fingerprint"])
    return out


def audit(hmm_dir: Path | str, since: date, expect_context_rows: Optional[int] = None) -> Audit:
    hmm_dir = Path(hmm_dir)
    a = Audit()
    con = duckdb.connect()
    disk = _fingerprints_on_disk(hmm_dir / "models")
    stores = {"market": hmm_dir / "market_regime.parquet", "macro": hmm_dir / "macro_regime.parquet"}
    for name, path in stores.items():
        if not path.exists():
            a.problems.append(f"{name}: store {path} is missing")
            continue
        p = str(path)
        if _has_col(con, p, "model_train_end"):
            a.oos_seen_by_model[name] = _q(con, "SELECT COUNT(*) FROM read_parquet($p) "
                                                "WHERE NOT in_sample AND model_train_end >= date", p=p)[0][0]
            a.unverifiable_legacy_oos[name] = _q(con, "SELECT COUNT(*) FROM read_parquet($p) "
                                                      "WHERE NOT in_sample AND model_train_end IS NULL", p=p)[0][0]
        else:
            a.oos_seen_by_model[name] = 0
            a.unverifiable_legacy_oos[name] = _q(con, "SELECT COUNT(*) FROM read_parquet($p) "
                                                      "WHERE NOT in_sample", p=p)[0][0]
        if a.oos_seen_by_model[name] > 0:
            a.problems.append(f"A1 {name}: {a.oos_seen_by_model[name]} out-of-sample row(s) were scored by a "
                              f"model that had seen them (model_train_end >= date)")
        a.accounting[name] = _q(con, "SELECT in_sample, COUNT(*), MIN(date), MAX(date) FROM read_parquet($p) "
                                     "GROUP BY in_sample ORDER BY in_sample", p=p)
        seen = {r[0] for r in _q(con, "SELECT DISTINCT model_config_fingerprint FROM read_parquet($p) "
                                      "WHERE date > $since", p=p, since=since)}
        a.fingerprints[name] = {"on_new_rows": sorted(seen), "on_disk": sorted(disk[name])}
        if seen and not seen <= disk[name]:
            a.problems.append(f"A4 {name}: new rows carry fingerprint(s) {sorted(seen - disk[name])} that are not "
                              f"on disk ({sorted(disk[name])})")
    ctx = hmm_dir / "context_regime.parquet"
    if ctx.exists():
        a.context_both_oos_since = _q(con, "SELECT COUNT(*) FROM read_parquet($p) WHERE NOT macro_in_sample "
                                           "AND NOT market_in_sample AND date > $since", p=str(ctx), since=since)[0][0]
        if expect_context_rows is not None and a.context_both_oos_since != expect_context_rows:
            a.problems.append(f"A3: {a.context_both_oos_since} context row(s) with both sides out-of-sample after "
                              f"{since}, expected {expect_context_rows}")
    else:
        a.problems.append(f"context_regime store {ctx} is missing")
    return a


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="HMM catch-up audit (ADR-058 HMM-R01 step 3)")
    ap.add_argument("--hmm-dir", default="data/gold/hmm")
    ap.add_argument("--since", required=True, type=date.fromisoformat, help="last date scored before the catch-up")
    ap.add_argument("--expect-context-rows", type=int)
    args = ap.parse_args(argv)
    a = audit(args.hmm_dir, args.since, args.expect_context_rows)
    for name in ("market", "macro"):
        print(f"{name}: A1 oos_seen_by_model={a.oos_seen_by_model.get(name)} "
              f"unverifiable_legacy_oos={a.unverifiable_legacy_oos.get(name)}")
        for row in a.accounting.get(name, []):
            print(f"  A2 in_sample={row[0]} n={row[1]} {row[2]}..{row[3]}")
        print(f"  A4 {a.fingerprints.get(name)}")
    print(f"context: A3 both-sides-out-of-sample rows after {args.since}: {a.context_both_oos_since}")
    for p in a.problems:
        print(f"PROBLEM {p}")
    print("AUDIT", "PASS" if a.passed else "FAIL")
    return 0 if a.passed else 1


if __name__ == "__main__":
    sys.exit(main())
