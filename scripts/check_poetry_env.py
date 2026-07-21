"""
scripts/check_poetry_env.py
ADD ADR-026 — asserts what the setup sequence has always assumed but
never checked: `poetry install --with dev` lands inside the active
conda env, not a separate Poetry-managed one.
"""
from __future__ import annotations
import argparse, os, re, subprocess, sys
from pathlib import Path

ENV_YAML = Path(__file__).resolve().parents[1] / "environment.yml"
_NAME_RE = re.compile(r"^name:\s*(\S+)", re.MULTILINE)

def _expected_env_name() -> str:
    m = _NAME_RE.search(ENV_YAML.read_text())
    if not m:
        raise RuntimeError(f"no 'name:' field found in {ENV_YAML}")
    return m.group(1)

def _check_pre(expected: str) -> int:
    active = os.environ.get("CONDA_PREFIX")
    active_name = Path(active).name if active else None
    if active_name != expected:
        got = f" (active: '{active_name}')" if active_name else ""
        print(f"FAIL: conda env '{expected}' is not active{got}.")
        print(f"      Run: conda activate {expected}")
        return 1
    print(f"PASS: conda env '{expected}' active at {active}")
    return 0

def _check_post(expected: str) -> int:
    active = os.environ.get("CONDA_PREFIX", "")
    resolved = subprocess.run(
        ["poetry", "env", "info", "--path"], capture_output=True, text=True
    ).stdout.strip()
    if resolved != active:
        print(f"FAIL: Poetry installed into '{resolved}', not '{active}'. "
              f"A separate virtualenv was created — plain `python`/`pytest` "
              f"will NOT see the installed packages.")
        return 1
    print(f"PASS: Poetry installed directly into the active conda env ({resolved})")
    return 0

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    # FIX ADR-026: --pre must be a real flag, not just "absence of --post" —
    # the Makefile (setup/install/doctor targets) calls this script with
    # --pre explicitly. Declaring it makes that call valid and the intent
    # self-documenting; behavior is unchanged (no flag still defaults to
    # the pre-check, same as before).
    p.add_argument("--pre", action="store_true",
                   help="Check before `poetry install` (conda env active)")
    p.add_argument("--post", action="store_true",
                   help="Check after `poetry install` (Poetry env == active conda env)")
    args = p.parse_args()
    expected = _expected_env_name()
    return _check_post(expected) if args.post else _check_pre(expected)

if __name__ == "__main__":
    sys.exit(main())
