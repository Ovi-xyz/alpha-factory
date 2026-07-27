"""
scripts/check_poetry_env.py
ADD ADR-026 — asserts what the setup sequence has always assumed but
never checked: `poetry install --with dev` lands inside the active
conda env, not a separate Poetry-managed one.

FIX ADR-028: a prerequisite this script never checked either — that
`poetry` is even installed at all. See alpha-factory_logs.txt: `poetry
install --with dev` right after `conda activate alpha-factory` failed
with a bare `zsh: command not found: poetry`, before this script's own
--pre check (or Makefile's `setup`/`install`/`doctor` targets, which all
call --pre first) ever got a chance to say anything useful. Nothing in
README.md, environment.yml, or Makefile installed Poetry itself anywhere
in the documented flow — every fix through ADR-026 assumed it as a given.
Separately, _check_post()'s raw subprocess.run(["poetry", ...]) call
would itself raise an uncaught FileNotFoundError in the same situation —
worse than the shell's own error, not better. Both are fixed by the same
_poetry_available() guard below.
"""
from __future__ import annotations
import argparse, os, re, shutil, subprocess, sys
from pathlib import Path

ENV_YAML = Path(__file__).resolve().parents[1] / "environment.yml"
_NAME_RE = re.compile(r"^name:\s*(\S+)", re.MULTILINE)

POETRY_INSTALL_HINT = (
    "      Run: pip install poetry\n"
    "      (conda envs are not externally-managed, so plain pip works here —\n"
    "      no --break-system-packages / --user needed inside the activated\n"
    "      env). Prefer an isolated global install instead? Use pipx install\n"
    "      poetry, or see https://python-poetry.org/docs/#installation."
)

def _poetry_available() -> bool:
    return shutil.which("poetry") is not None

def _expected_env_name() -> str:
    m = _NAME_RE.search(ENV_YAML.read_text())
    if not m:
        raise RuntimeError(f"no 'name:' field found in {ENV_YAML}")
    return m.group(1)

def _check_pre(expected: str) -> int:
    # FIX ADR-028: checked before the conda-env check below — an inactive
    # env is fixable in one command either way, but a missing `poetry`
    # command makes every subsequent step in `setup`/`install` fail with
    # a shell error that names no fix at all.
    if not _poetry_available():
        print("FAIL: `poetry` command not found on PATH.")
        print(POETRY_INSTALL_HINT)
        return 1

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
    # FIX ADR-028: guard before the subprocess call, which previously
    # raised an uncaught FileNotFoundError here if poetry vanished (or
    # was never installed) between --pre and --post.
    if not _poetry_available():
        print("FAIL: `poetry` command not found on PATH.")
        print(POETRY_INSTALL_HINT)
        return 1

    active = os.environ.get("CONDA_PREFIX", "")
    try:
        resolved = subprocess.run(
            ["poetry", "env", "info", "--path"], capture_output=True, text=True
        ).stdout.strip()
    except FileNotFoundError:
        print("FAIL: `poetry` command not found on PATH.")
        print(POETRY_INSTALL_HINT)
        return 1
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
