# Upgrade plan — book-crypto-data

Score: 7/10 -> 8/10 — lake tests now run standalone in CI via the pinned `[lake]` extra; remaining gaps are provider-parser fixtures and packaging polish.

## Backlog

- P0: Confirm the first CI run on GitHub is green (it now downloads the pinned
  `solo-empire-data-lake` tarball); keep it required on `main`.
- P1: When `solo-empire-data-lake` moves, bump the pinned commit in `[lake]` together with
  the other book-*-data repos (same SHA everywhere).
- P2: Add a `[project.optional-dependencies] dev` extra and a `[build-system]` table so
  `pip install -e ".[dev]"` is the single documented setup.
- P2: Decide whether `archive/` is still needed; it is not packaged or tested.

## Done in this pass (pass 2)

- Added a `[lake]` extra pinning `solo-empire-data-lake` at `68fb5a9` (plus pyarrow/duckdb); CI
  installs `-e ".[lake]"`, asserts `data_lake` imports, and lake tests now run instead of skipping.
- Test guards use `lake.shared_runtime_available()` (`importlib.util.find_spec("data_lake")`,
  then the `SOLO_EMPIRE_ROOT` / parent-checkout fallback) instead of `find_solo_empire_root()`.
- `[tool.ruff.lint] select = ["E4", "E7", "E9", "F"]` pins the classic rule set: unpinned
  ruff 0.16 widened its defaults and would have failed `ruff check .` in CI.
- `tests/test_fixture_headers.py` pins `fixtures/*.csv` headers to the CSV projection.
- OHLCV lake tests also require pyarrow (they failed, not skipped, with `SOLO_EMPIRE_ROOT` but no pyarrow).
- Verified: fresh venv `pip install -e ".[lake]"` from the pinned tarball — 1 skipped of 51 with `[lake]` (parent-path discovery test only; was 34 of 50 skipped);
  also against `pip install /home/user/solo-empire-data-lake`, with `SOLO_EMPIRE_ROOT=<parent>`
  (parent adapter wins), and without the extra (lake tests skip with a reason). ruff 0.15 and 0.16 clean.

## Done in pass 1

- `lake.find_solo_empire_root()` now returns `None` when the shared `data_lake`
  adapter cannot be imported (it raised `ModuleNotFoundError` before), and the
  adapter loader also honours `SOLO_EMPIRE_ROOT` so a sibling clone of the parent
  repo works without editing `PYTHONPATH`.
- CI: installs `pytest ruff`, runs `ruff check .` and `python -m pytest -q -rs`
  (skip reasons visible) instead of bare `unittest`.
- Fixed the remaining default-rule ruff findings; `.gitignore` covers egg-info/ruff cache.
- README "Tests" section documents the standalone and full-lake commands.
- Verified: clean venv with `pip install -e . pytest` (CI shape) and a full run with
  `SOLO_EMPIRE_ROOT=<parent>` + pyarrow/duckdb (all lake tests execute and pass).
