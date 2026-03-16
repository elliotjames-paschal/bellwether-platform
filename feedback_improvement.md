# Feedback Pipeline Test Results & Improvements

**Date:** 2026-03-10
**Tester:** Automated test suite via Claude Code

---

## Test Results Summary

| Test | Description | Result | Notes |
|------|-------------|--------|-------|
| F | Dry-run apply | PASS | Pending count, action counts, DRY RUN message, no files modified |
| G | Verify _SPLIT removed | PASS | Only references in `migrate_split_tickers()` and test files. No code adds _SPLIT |
| H | Exclusion creation (different_event) | PASS | `match_exclusions.json` created with `exc_` prefix, sorted IDs, ticker, reason, source_label_id |
| I | Legacy _SPLIT migration | PASS (no-op) | No _SPLIT tickers exist. No migration message printed. Correct behavior |
| J | Validation safety (agent diff) | PASS | TRUMP vs HARRIS blocked with `needs_review`, reason: `core_differs (agent: TRUMP vs HARRIS)` |
| K | Generate accuracy report | PASS | Report written with matching metrics, disagreements, batch_id, suggested_labels schema |
| L | Empty labels evaluate | PARTIAL FAIL | Prints "No human labels found" but doesn't write a zeroed-out report. Stale report from previous run persists |
| M | Dry-run corrections | PASS | Shows FN/FP counts, correction/disambiguation counts, DRY RUN message |
| N | Per-type frequency override | PASS | Thresholds correctly overridden. No corrections generated (threshold diff not a correction type) |
| O | No report file | PASS | Prints "No accuracy report found. Run pipeline_evaluate_matches.py first." |
| P | Exclusions in market map | PARTIAL | Unicode arrow bug fixed. Script runs, loads exclusions. Cannot fully verify split behavior (test tickers not in enriched data) |
| Q | No exclusions file | PASS | Returns 0 exclusions, no crash |
| R | Corrections applied on next run | SKIPPED | Requires `tickers_all.json` from `create_tickers.py` (GPT-dependent) |
| S | Disambiguations applied on next run | SKIPPED | Same prerequisite as Test R |
| T | Missing corrections/disambiguations files | SKIPPED | Same prerequisite as Test R |
| U | Full pipeline Phase 5 | SKIPPED | Requires full pipeline with GPT API calls |
| V | Schema validation | PASS | All schemas valid: `hl_` prefixes, status values, label types, precision/recall bounds |
| W | Idempotency end-to-end | PASS | Ingest: "No new rows". Apply: 0 actions. Evaluate: identical metrics. Corrections: same |
| X1 | Unreachable source file | PARTIAL FAIL | Crashes with unhandled `FileNotFoundError` instead of friendly error message |
| X2 | Empty Markets (JSON) | PASS | Row skipped, no crash |
| X3 | Zero-market rows | PASS | Row skipped, no crash |
| X4 | Non-existent market_id | PASS | Label skipped with "missing tickers", `needs_review` |
| X5 | wrong_category no valid category | PASS | Status `needs_review`, reason: "no valid category in description" |
| X6 | tickers_postprocessed.json missing | PASS | Falls back to 0 tickers, no crash |
| X7 | Corrupt JSON | PASS (expected crash) | `JSONDecodeError` as expected. Protected by `atomic_write_json` in normal operations |
| X8 | Single market for same_event | PASS | Skipped with "single market" |
| X9 | N>2 markets, one pair has agent diff | PASS | Entire group blocked with `needs_review` |
| X10 | Unicode in descriptions | FAIL (fixed) | `atomic_write_json` didn't specify `encoding='utf-8'`. Fixed in `config.py` |

---

## Bugs Found & Fixed

### 1. Unicode encoding crash in `atomic_write_json` (CRITICAL)

**File:** `packages/pipelines/config.py:166`
**Issue:** `os.fdopen(fd, 'w')` defaults to Windows cp1252 encoding, which cannot encode CJK characters, emojis, or many accented characters. Any non-ASCII description in feedback would crash the entire pipeline.
**Fix:** Changed to `os.fdopen(fd, 'w', encoding='utf-8')`.
**Impact:** Affects ALL pipeline scripts that write JSON on Windows.

### 2. Unicode arrow in `generate_market_map.py` (MEDIUM)

**File:** `packages/pipelines/generate_market_map.py:210`
**Issue:** `→` character in print statement crashes on Windows cp1252.
**Fix:** Replaced `→` with `->`.

### 3. Unicode emoji in `pipeline_classify_electoral.py` (MEDIUM)

**File:** `packages/pipelines/pipeline_classify_electoral.py:318`
**Issue:** `⚠` character in log message crashes on Windows cp1252.
**Fix:** Wrapped `print()` in try/except with ASCII fallback encoding.

---

## Improvements Recommended

### High Priority

#### 1. Empty labels should produce a zeroed-out report (Test L) — FIXED
`pipeline_evaluate_matches.py` now writes a zeroed-out report (all metrics 0, empty disagreements/suggested_labels) when no labels exist, instead of leaving stale data on disk.

#### 2. File-not-found error handling in ingest (Test X1) — FIXED
`pipeline_ingest_feedback.py` now checks `Path.exists()` before opening and prints `ERROR: CSV file not found: <path>` with `sys.exit(1)`.

#### 3. Systematic Windows encoding audit — FIXED
- `config.py`: `atomic_write_json` now uses `encoding='utf-8'` in `os.fdopen`
- `pipeline_daily_refresh.py`: `run_script()` now sets `PYTHONIOENCODING=utf-8` in subprocess env and uses `encoding='utf-8'` + `errors='replace'` in `subprocess.run()`
- 44 `open()` calls across 22 daily pipeline scripts updated to include `encoding='utf-8'`

### Medium Priority

#### 4. Log "0 exclusions loaded" message in market map (Test Q)
`generate_market_map.py` only prints the exclusion count when > 0. Should always print for operational visibility.

#### 5. `threshold_mismatch` correction type not implemented (Test N)
The FN between `BWR-FED-CUT-RATES-STD-25BPS-2026` and `BWR-FED-CUT-RATES-STD-50BPS-2026` is a threshold difference, but `generate_ticker_corrections.py` only generates rules for mechanism, agent, target, and timeframe aliases. Threshold mismatches are classified but never produce correction rules. Consider whether threshold aliases make sense or if this is intentionally excluded.

#### 6. `other` label type has no apply handler
Labels with type `other` stay `pending` forever. Consider:
- Adding a mechanism to mark them as `acknowledged` after manual review
- Or auto-setting status to `needs_review` so they're clearly flagged

### Low Priority

#### 7. Coverage gaps not testable without full pipeline
Tests R, S, T, U require `tickers_all.json` (from `create_tickers.py`) which needs GPT API calls. These should be tested during a full pipeline run. Consider creating minimal test fixtures that can be used without GPT.

#### 8. Untested scenarios (from Section 6)
- Contradictory labels (same pair, one says same_event, other says different_event) — undefined behavior
- Concurrent pipeline runs — `atomic_write_json` helps but CSV writes are not atomic
- Master CSV write atomicity — `apply_not_political` and `apply_wrong_category` use direct `pd.to_csv()`, not atomic write
- Exclusion + unification conflict — label A excludes (m1, m2), label B unifies (m1, m2) — which wins depends on processing order

---

## Files Modified During Testing

| File | Change |
|------|--------|
| `packages/pipelines/config.py` | Added `encoding='utf-8'` to `atomic_write_json` |
| `packages/pipelines/generate_market_map.py` | Replaced `→` with `->` in print |
| `packages/pipelines/pipeline_classify_electoral.py` | Added Unicode-safe `log()` function |
| `packages/pipelines/pipeline_select_election_winners.py` | Added `get_openai_api_key()` import and usage |

## Test Data Files Created

All in `data/`:
- `qa_test.csv` — 6 rows covering all label types
- `qa_test_h.csv` — different_event with same-ticker pair
- `qa_test_j.csv` — agent mismatch safety test
- `qa_test_x2.csv`, `qa_test_x2b.csv` — empty/zero market edge cases
- `qa_test_x4.csv` — non-existent market IDs
- `qa_test_x5.csv` — wrong_category with invalid description
- `qa_test_x8.csv` — single market for same_event
- `qa_test_x9.csv` — N>2 markets with agent diff
- `qa_test_x10.csv` — Unicode stress test
- `test_feedback.csv` — downloaded from live Google Sheet
