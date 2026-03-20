# QA Audit Report - Bellwether Platform Data

**Audit Date:** 2026-03-20
**Auditor:** Claude (QA Engineer)
**Status:** In Progress

---

## 1. EMPTY / NEAR-EMPTY DATA

| # | File | Issue | Severity | Status |
|---|------|-------|----------|--------|
| 1.1 | `election_winner_stats.json` | Completely empty `{}` - 2 bytes. Upstream pipeline (`election_winner_markets_comparison.py`) has 0 matched winner markets - all 57 selections are future/unresolved elections. Frontend handles gracefully (shows dashes). | LOW | [x] WONTFIX - expected given data state |
| 1.2 | `acled_events.json` | Events array is empty `[]`. Only contains color config. Generated 2026-02-10 (38 days stale). Requires ACLED API credentials. | LOW | [x] WONTFIX - needs external API credentials |
| 1.3 | `civic_elections.json` | Only 1 election (VA Special Election) with 0 contests and 0 matched markets. Generated from Google Civic API. | LOW | [x] WONTFIX - correct for current election calendar |

---

## 2. STALE DATA

| # | File | `generated_at` | Days Stale | Severity | Status |
|---|------|---------------|------------|----------|--------|
| 2.1 | `monitor_elections.json` | 2026-02-06 | 42 days | ~~CRITICAL~~ NONE | [x] DELETED - orphan file, not referenced by any frontend code |
| 2.2 | `monitor_markets.json` | 2026-02-03 | 45 days | ~~CRITICAL~~ NONE | [x] DELETED - orphan file, not referenced by any frontend code |
| 2.3 | `acled_events.json` | 2026-02-10 | 38 days | LOW | [x] See 1.2 |
| 2.4 | `liquidity_timeseries.json` | Ends 2026-02-21 | 27 days | HIGH | [ ] Will be fixed by pipeline re-run (phase 4-5) |
| 2.5 | Kalshi liquidity timeseries | Kalshi data ends at 2026-02-06 (all nulls after), then 31 trailing null entries | HIGH | [ ] Will be fixed by pipeline re-run (phase 4-5) |

---

## 3. DATA CORRECTNESS & CONSISTENCY

### 3.1 Cross-file market count mismatches (HIGH)

| Metric | summary.json | platform_stats.json | market_distribution.json | Current master CSV |
|--------|-------------|--------------------|-----------------------|-------------------|
| PM markets | 43,167 | 40,783 | 40,998 | **40,998** |
| K markets | 27,281 | 6,844 | 6,893 | **6,893** |
| Total | 70,448 | 47,627 | 47,891 | **47,891** |

**Root cause:** `summary.json` was generated from an older/larger dataset. `generate_summary_stats()` cannot regenerate because it depends on `polymarket_prediction_accuracy_all_political.csv` and `kalshi_prediction_accuracy_all_political.csv`, which are **missing** (produced by `calculate_all_political_brier_scores.py` which OOM'd on the 3.7GB server).

**Fix:** Added 4GB swap to Hetzner server. Pipeline re-run (phase 4-5) will regenerate all files from current master CSV.

**Status:** [ ] Awaiting pipeline re-run

### 3.2 Kalshi resolved markets = 0 (HIGH)

`platform_stats.json` shows Kalshi has **0 resolved markets**. Current master CSV shows K resolved=6,844.

**Status:** [ ] Same root cause as 3.1 - will be fixed by pipeline re-run

### 3.3 active_markets.json quality (MEDIUM)

- 87.8% of markets have null PM price, 88.0% null K price
- Only 0.8% (167) have both PM and K prices
- 60.4% have zero total volume
- 76.6% missing tickers (15,083 "no_ticker_fallback")

**Status:** [x] BY DESIGN - each market exists on one platform (PM or K), so null on the other is expected. The 167 cross-platform entries are the ones with both. Zero volume is common for illiquid markets.

### 3.4 monitor_summary: 99.6% fragile (MEDIUM)

Only 4/19,693 markets rated "robust" (0.02%), 83 "caution", 19,606 "fragile".

**Status:** [x] BY DESIGN - robustness is based on orderbook depth (cost to move price 5c). Most markets genuinely have thin orderbooks. The 4 robust + 83 caution markets are the reportable ones shown on the monitor page.

### 3.5 globe_elections.json: 100% missing country_code (~~MEDIUM~~ NONE)

274 of 274 elections have no `country_code` field.

**Status:** [x] NOT A BUG - `country_code` is not used by any frontend JS. Globe uses lat/lng for positioning. All 274 elections have valid lat/lng.

### 3.6 globe_markets.json: 100% missing lat/lon (~~NEEDS INVESTIGATION~~ NONE)

**Status:** [x] FALSE ALARM - All 393 globe market points DO have valid lat/lng. Initial audit incorrectly tested against active_markets.json field names. Active markets: 19,324/19,693 have coordinates (only 369 missing = 1.9%).

### 3.7 Category naming inconsistency (LOW)

"Economic Data" appears in `aggregate_statistics.json` but not in `active_markets.json` category_counts.

**Status:** [ ] Minor - different pipeline stages use different category mappings. Not user-visible.

---

## 4. STATISTICAL SMALL-N ISSUES

### 4.1 Brier by Category - misleading with tiny N (~~HIGH~~ ALREADY FIXED IN CODE)

Code already has `MIN_N = 5` threshold at `generate_web_data.py:815` and `:876`. Categories with n<5 get `null` Brier scores. Current data files are stale (generated before threshold was added).

**Status:** [x] Code fix already exists. Pipeline re-run will apply it to regenerate the JSON files.

### 4.2 Partisan Bias Calibration - all bins too small (MEDIUM)

Every bin has n=9-14. This is a data volume limitation, not a bug. As more elections resolve, bins will grow.

**Status:** [x] ACCEPTED - inherent limitation of current dataset size

### 4.3 Partisan Bias Regression - no significant results (LOW)

R²=0.015-0.029. No variable reaches p<0.05. Valid to show as a null result.

**Status:** [x] ACCEPTED - showing null results is informative

### 4.4 Calibration total only 621 predictions (LOW)

Split across 20 quantile bins of ~31 each. Thin but acceptable.

**Status:** [x] ACCEPTED

---

## 5. OTHER ANOMALIES

| # | Issue | Severity | Status |
|---|-------|----------|--------|
| 5.1 | Kalshi liquidity spread spikes to 120% in Dec 2025 | LOW | [x] ACCEPTED - reflects real Kalshi orderbook conditions during platform transition |
| 5.2 | Candidacy category: 135 markets, $0.9M total volume, avg $6.7K | LOW | [x] ACCEPTED - niche category with genuinely low volume |
| 5.3 | Economic Data category median volume = $400 | LOW | [x] ACCEPTED - mostly Kalshi markets with low activity |

---

## Remaining Action Items

1. **Re-run pipeline phase 4-5** on Hetzner (swap added, awaiting re-run) → fixes 2.4, 2.5, 3.1, 3.2, 4.1
2. **Rotate OpenAI API key** (exposed in chat) → security fix
3. **Fix Brier OOM** (DONE - added 4GB swap)
4. **Delete orphan files** (DONE - removed monitor_elections.json, monitor_markets.json)

---

## Fix Log

| Date | Issue # | Action Taken | Verified on Live |
|------|---------|-------------|-----------------|
| 2026-03-20 | 2.1, 2.2 | Deleted orphan files `monitor_elections.json` and `monitor_markets.json` | [ ] |
| 2026-03-20 | Fix 1 | Added 4GB swap on Hetzner (`/swapfile2`) to prevent Brier OOM | N/A (infra) |
| 2026-03-20 | 3.1-4.1 | Pipeline phase 4-5 re-run initiated on Hetzner | [ ] |
