# Upgrade plan — book-crypto-data

Score: 8/10 -> 8.5/10 — rows with NaN/inf/negative/duplicate values are now rejected before Bronze/CSV, projections are written atomically, and CLI input is validated; remaining gaps are packaging polish.

## Backlog

- P0: Confirm the first CI run on GitHub is green (it now downloads the pinned
  `solo-empire-data-lake` tarball); keep it required on `main`.
- P1: When `solo-empire-data-lake` moves, bump the pinned commit in `[lake]` together with
  the other book-*-data repos (same SHA everywhere).
- P2: Add a `[project.optional-dependencies] dev` extra and a `[build-system]` table so
  `pip install -e ".[dev]"` is the single documented setup.
- P2: Decide whether `archive/` is still needed; it is not packaged or tested.
- P1: Validate the trending payload shape (`coins[].item` keys) instead of raising
  `KeyError` inside `fetch_trending`; today it is caught as best-effort.
- P2: Surface the per-run `rejected_by_reason` counts in `/v1/metadata`.

## Done in this pass (pass 3)

- New `quality` module (`finite_number`, `non_negative_number`, `dedupe_by_key`):
  `lake.price_records_with_report` drops rows with missing/non-numeric/NaN/inf/negative
  prices and duplicate `coin:currency` ids, blanks bad volume/market cap, and a
  non-finite 24h change becomes 0; the CSV projection applies the same filter.
  Rejection counts go to stdout and landing metadata; zero valid rows fail before the lake write.
- `ohlcv.kline_records` drops malformed/inconsistent bars and repeated open times;
  `funding_records` drops non-finite rates and repeats (empty pages no longer write).
- New `fsutil` module: price/trending CSVs are replaced atomically and history is
  appended via an atomic rewrite.
- CLI: `--coins`/`--vs-currencies` are lowercased/de-duplicated and must be non-empty;
  `--alert-threshold` must be finite and >= 0; `ohlcv --days` must be > 0 (exit 2).
- README: Quick start uses `pip install -e ".[lake]"`; new "Data quality" section.
- Verified: `pytest -q -rs` 65 passed / 1 skipped (was 50/1) with the `[lake]` venv;
  `--fixture` smoke run; ruff 0.15.8 and 0.16.9 clean.

## Done in pass 2

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
