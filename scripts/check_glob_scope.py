"""
scripts/check_glob_scope.py — CI Gate G-8: Layer 1/Layer 2 glob-scope enforcement

ADD ADR-022 (GMI_Decision_Document_v2.docx, 2026-07-11): statically scans
src/ for two violation classes, closing GMI_Implementation_Checkpoint_v3.docx
Section 11.1/11.3's two open audit questions as a permanent, repo-wide gate
rather than a manually re-verified one-time finding:

  (a) Any read_parquet()/scan_parquet() glob-pattern STRING LITERAL
      containing two or more '**' segments in one path string -- DuckDB's
      read_parquet() raises "Cannot use multiple '**' in one path" on
      this (RISK-2's defect class; empirically confirmed in
      GMI_Implementation_Checkpoint_v2.docx Section 4.3).

  (b) Any glob path rooted at market_ohlcv that is NOT constructed via
      src/utils/silver_scope.py's layer1_globs() / context_glob() helpers
      -- i.e. a string literal of the shape ".../market_ohlcv/**/..."
      anywhere in live code (a module constant, a dict/list/tuple
      element, a function call argument, a DuckDB SQL string embedding
      read_parquet(...) as text). This is RISK-6's bug class (Layer 1
      quality/signal checks silently also scanning Layer 2 rows sharing
      the same directory root) -- two known instances
      (quality_validator.py, technical_signals.py) were fixed by hand in
      the Bronze/Silver Solidification thread; this gate exists because
      that fix was manual code-reading, not an exhaustive audit, and the
      project has already had to upgrade a CI gate twice before on
      exactly this "found by hand, not caught by CI" pattern (G-2,
      rewritten after FIX GLD-006; G-4, added after NEW-4).

DESIGN NOTE -- why AST, not regex (unlike Gate G-2's f-string scanner):
an early regex draft of this scanner false-positived on
technical_signals.py's own module docstring, which discusses the OLD
broken glob path as history/documentation, not a live construction. This
scanner parses each file with `ast` and inspects every string constant
EXCEPT those that are the sole value of a bare `ast.Expr` statement
(module/class/function docstrings, and any other no-op bare string
statement) -- those are excluded by construction, not by a fragile
"does this line start with #" text heuristic. Everything else (assignment
RHS, dict values, list/tuple elements, call arguments, f-string constant
fragments) is in scope, since a glob rooted at market_ohlcv can legally
appear in any of those shapes (views.py embeds one inside a DuckDB
CREATE VIEW SQL string that is itself a dict value, for example).

silver_scope.py itself is explicitly exempted -- it is the one file that
legitimately constructs these globs, one '**' per market subdirectory.

Usage:
    python scripts/check_glob_scope.py          # human-readable report
    python scripts/check_glob_scope.py --quiet  # CI mode: silent on pass

Exit code 0 = no violations. Exit code 1 = violations found (printed to
stdout regardless of --quiet).
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

_EXEMPT_FILES = frozenset({
    "src/utils/silver_scope.py",
})

_DOUBLE_GLOBSTAR = re.compile(r"\*\*.*\*\*")
_UNSCOPED_MARKET_OHLCV = re.compile(r"market_ohlcv[\\/]?(\{[^}]*\}[\\/]?)?\*\*")


def _docstring_like_node_ids(tree):
    """
    Identify string Constant nodes that are the sole value of a bare
    `ast.Expr` statement -- module/class/function docstrings, or any other
    no-op bare string statement. Returns a set of `id()` values for those
    Constant nodes so the main walk can skip exactly them and nothing else.
    """
    excluded = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) \
                and isinstance(node.value.value, str):
            excluded.add(id(node.value))
    return excluded


def _all_string_constants_with_line(tree, exclude_ids):
    """Every string Constant node in the tree, except excluded (docstring-like)
    ones, as (line_number, value) pairs. Also merges JoinedStr (f-string)
    constant fragments so f"...market_ohlcv/**/..." literal portions are seen."""
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in exclude_ids:
                continue
            found.append((node.lineno, node.value))
        elif isinstance(node, ast.JoinedStr):
            parts = [
                p.value for p in node.values
                if isinstance(p, ast.Constant) and isinstance(p.value, str)
            ]
            if parts:
                found.append((node.lineno, "".join(parts)))
    return found


def scan():
    violations = []

    for f in sorted(Path("src").rglob("*.py")):
        rel = f.as_posix()
        if rel in _EXEMPT_FILES:
            continue

        source = f.read_text(encoding="utf-8", errors="replace")
        try:
            tree = ast.parse(source, filename=rel)
        except SyntaxError:
            # Gate G-1 (syntax validation) already covers this -- not this
            # gate's concern. Skip rather than double-report.
            continue

        exclude_ids = _docstring_like_node_ids(tree)
        seen_this_file = set()  # (line_no, literal) dedup -- a Constant and
                                  # its containing JoinedStr can overlap lines

        for line_no, literal in _all_string_constants_with_line(tree, exclude_ids):
            if "market_ohlcv" not in literal or "*" not in literal:
                continue
            key = (line_no, literal)
            if key in seen_this_file:
                continue
            seen_this_file.add(key)

            if _DOUBLE_GLOBSTAR.search(literal):
                violations.append(
                    f"{rel}:{line_no}: double-'**' glob literal: {literal!r} "
                    f"-- DuckDB read_parquet() raises on this (RISK-2 defect class)"
                )
                continue

            if _UNSCOPED_MARKET_OHLCV.search(literal):
                violations.append(
                    f"{rel}:{line_no}: unscoped market_ohlcv glob: {literal!r} "
                    f"-- use src/utils/silver_scope.py's layer1_globs()/context_glob() "
                    f"instead of a raw '**' glob rooted at market_ohlcv (RISK-6 defect class)"
                )

    return violations


def main():
    quiet = "--quiet" in sys.argv
    violations = scan()

    if violations:
        print(f"Gate G-8 FAILED -- {len(violations)} glob-scope violation(s):")
        for v in violations:
            print(f"  {v}")
        print()
        print(
            "FIX: route market_ohlcv reads through "
            "src/utils/silver_scope.py's layer1_globs(silver_root, pattern) "
            "for Layer 1-only scope, or context_glob(silver_root, pattern) "
            "for Layer 2-only scope. See that module's docstring for the "
            "masking-bug history this gate guards against."
        )
        return 1

    if not quiet:
        print("Gate G-8 PASSED -- 0 glob-scope violations in src/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
