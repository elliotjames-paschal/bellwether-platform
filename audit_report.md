# Bellwether Pipeline Audit Report

Generated: February 25, 2026
Model: gpt-5.2
Files audited: 58
Total cost: $0.89

---

## 1. EXECUTIVE SUMMARY

Overall, the Bellwether Platform pipeline is functionally rich and demonstrates strong domain understanding (e.g., staged GPT classification with verification/tiebreak, pragmatic reference-date methodology for accuracy, and a growing audit/reporting layer). The codebase covers a wide surface area—market discovery, enrichment, pricing, truncation, resolution, analytics, web-data generation, and Cloudflare Workers—while maintaining generally readable, script-oriented implementations. Several components are notably well-designed: the staged GPT pipelines, the audit validator/report structure, and the use of backups prior to destructive truncation.

However, the system currently carries **material correctness and reliability risks** that can silently degrade outputs or block the pipeline. The most serious issues fall into five buckets: **(1) data integrity/keying problems** (unstable checkpoint keys, ID namespace mixing, non-atomic writes), **(2) time handling and cutoff inconsistencies** (timezone parsing, election-day truncation mismatch), **(3) “full refresh” / incremental logic defects** in price and trade pullers, **(4) cross-script composition failures** (directory/path mismatches causing web outputs not to generate), and **(5) Worker-side correctness/security gaps** (API key in query string, insecure dev fallback, missing timeouts/retries, and inconsistent volume/size semantics affecting VWAP and combined pricing).

Top-level recommendation: **prioritize a stabilization sprint** focused on (a) correctness of core datasets (prices, resolutions, election metadata, brier inputs), (b) atomicity and stable identifiers across checkpoints and indices, (c) consistent file location conventions and schema contracts between producer/consumer scripts, and (d) Worker reliability/security hardening. After these are addressed, invest in modest packaging/architecture improvements (shared utilities for time/cutoffs, atomic writes, schema validation, and rate limiting) to reduce drift across scripts and between the two Workers.

---

## 2. CRITICAL & HIGH FINDINGS (Consolidated)

> Line numbers are approximate (±10) per audit notes.

### CH-01 — Merge step can crash on `winning_outcome` normalization
- **Severity:** CRITICAL  
- **File:Line:** `packages/pipelines/pipeline_merge_to_master.py:~235-245`  
- **Description:** Uses `wo.str.lower()` on a column that may contain NaN/mixed types, risking `AttributeError` and hard-stopping the pipeline.  
- **Recommendation:** Cast to pandas `string` dtype or `fillna('')` before `.str` ops; add a schema assertion for `winning_outcome`.

### CH-02 — Election dates lookup first-run crash + key collision risk
- **Severity:** CRITICAL / HIGH  
- **File:Line:** `packages/pipelines/pipeline_get_election_dates.py:~330-410`  
- **Description:** On first run, `dates_df` is created without `country` (and `is_primary`), then later referenced → `KeyError`. Additionally, lookup keys omit `is_primary`, colliding primary/general elections.  
- **Recommendation:** Initialize lookup schema with `['country','office','location','election_year','is_primary','election_date']` and include `is_primary` in the key consistently.

### CH-03 — Polymarket & Kalshi price pullers: `--full-refresh` does not behave as advertised
- **Severity:** CRITICAL  
- **File:Line:**  
  - `packages/pipelines/pull_polymarket_prices.py:~170-175,~214-216`  
  - `packages/pipelines/pull_kalshi_prices.py:~197-205,~279-281`  
- **Description:** “Full refresh” is logged but does not reliably bypass skip logic / start-time logic. Kalshi incremental mode can also backfill from 2020 when no existing data, causing huge pulls and rate-limit/timeout behavior.  
- **Recommendation:** Thread `full_refresh` into `process_market()` and explicitly set start timestamps for incremental vs full; add tests asserting behavior.

### CH-04 — Non-atomic overwrites of core JSON outputs (corruption risk)
- **Severity:** HIGH  
- **File:Line:** Multiple, including:  
  - `pull_polymarket_prices.py:~245-247`  
  - `pull_kalshi_prices.py:~245-247`  
  - `truncate_polymarket_prices.py:~250-252`  
  - `truncate_kalshi_prices.py:~356-358`  
  - `fetch_orderbooks.py:~318-322,~368-372`  
  - `fetch_panel_a_trades.py:~235-238,~269-272`  
  - `fix_name_collisions.py:~268-276`  
- **Description:** Direct in-place writes can corrupt files on interruption, breaking subsequent loads and silently losing data.  
- **Recommendation:** Standardize `write_json_atomic(path, data)` (temp file + `os.replace()`), use it everywhere, and consider fsync for critical artifacts.

### CH-05 — Web data generation reads from wrong directories; outputs silently missing
- **Severity:** CRITICAL / HIGH  
- **File:Line:** `packages/pipelines/generate_web_data.py:~980-1260`  
- **Description:** Several functions read from `DATA_DIR` while producer scripts write to `PAPER_DATA_DIR` (e.g., shared elections detailed CSV, cohort CSV, election winner panel files). This likely prevents platform comparison, convergence, and regression JSON from being generated.  
- **Recommendation:** Standardize producer/consumer paths (single source of truth), add explicit “required input missing” hard failures (or loud warnings) listing the producing script.

### CH-06 — Calibration density plots can crash on small datasets (division by zero)
- **Severity:** CRITICAL  
- **File:Line:** `packages/pipelines/calibration_density_plots.py:~135-165`  
- **Description:** `samples_per_bin = len(df)//num_bins` can be 0 when sample size < bins, causing a crash.  
- **Recommendation:** Use `qcut` with `min(num_bins, n)` and `duplicates='drop'`, or clamp `samples_per_bin >= 1`.

### CH-07 — Partisan bias regression uses degenerate dependent variable (misleading results)
- **Severity:** CRITICAL  
- **File:Line:** `packages/pipelines/table_partisan_bias_regression.py:~35-90`  
- **Description:** Sets `prediction_error = winner_prediction - 1.0` where `winner_prediction` is already “probability of the realized winner,” making the regression test underconfidence-in-winner rather than partisan bias.  
- **Recommendation:** Recompute a consistent Republican probability and regress `republican_prob - republican_won` (or equivalent contract-level error).

### CH-08 — Discovery pagination termination can truncate Kalshi results
- **Severity:** HIGH  
- **File:Line:** `packages/pipelines/pipeline_discover_markets_v2.py:~70-200`  
- **Description:** Stops pagination on `len(markets) < limit` in cursor-based APIs, which can truncate results.  
- **Recommendation:** Terminate strictly based on cursor semantics per API docs; add logging for page counts and final cursor.

### CH-09 — Category classification checkpoint keyed by DataFrame index (silent misapplication)
- **Severity:** HIGH  
- **File:Line:** `packages/pipelines/pipeline_classify_categories.py:~520-610`  
- **Description:** Checkpoint keys based on row index; if CSV ordering changes, classifications can be applied to the wrong market.  
- **Recommendation:** Key checkpoints by stable IDs (`pm_condition_id`, Kalshi ticker) with platform prefix.

### CH-10 — Reclassify incomplete uses non-deterministic `hash()` and passes wrong args to electoral pipeline
- **Severity:** HIGH / CRITICAL (correctness)  
- **File:Line:** `packages/pipelines/pipeline_reclassify_incomplete.py:~70-95,~360-390`  
- **Description:** Uses Python `hash()` (salted per run) for IDs; also calls `run_electoral_pipeline` positionally such that `market_ids`/`scheduled_end_times` are not passed, degrading election-year inference and consistency.  
- **Recommendation:** Use deterministic hash (sha256) and pass `market_ids=` and `scheduled_end_times=` explicitly.

### CH-11 — Resolution inference for Polymarket is unreliable
- **Severity:** HIGH  
- **File:Line:** `packages/pipelines/pipeline_check_resolutions.py:~150-230`  
- **Description:** Infers winner from `outcomePrices >= 0.99` when closed; can be wrong due to rounding/fees/stale prices and ignores explicit resolution fields.  
- **Recommendation:** Prefer authoritative resolution fields/endpoints; only fall back to price heuristics with explicit confidence flags.

### CH-12 — `fetch_resolution_prices.py` parses close times as local timezone (wrong cutoff)
- **Severity:** HIGH  
- **File:Line:** `packages/pipelines/fetch_resolution_prices.py:~63-76`  
- **Description:** Strips timezone markers and creates naive datetime; `timestamp()` interprets in local system timezone, shifting close time and selecting wrong “last candle before close.”  
- **Recommendation:** Parse as UTC-aware (`pd.to_datetime(..., utc=True)`), and enforce tz-aware comparisons.

### CH-13 — Truncation “election day” cutoff is UTC and inconsistent across scripts
- **Severity:** HIGH  
- **File:Line:**  
  - `truncate_polymarket_prices.py:~33-36,~170-176`  
  - `truncate_kalshi_prices.py:~198-205`  
  - `pull_trades_for_vwap.py:~63-66,~120-132`  
- **Description:** Truncation uses 23:59:59 UTC; VWAP script references 00:00 UTC election day; neither accounts for local election day. This misaligns windows and can bias metrics.  
- **Recommendation:** Centralize cutoff computation in a shared module; decide and document whether cutoffs are local or UTC; apply consistently.

### CH-14 — `truncate_kalshi_prices.py` hard-coded BASE_DIR (non-portable)
- **Severity:** CRITICAL  
- **File:Line:** `packages/pipelines/truncate_kalshi_prices.py:~26-29`  
- **Description:** Hard-coded user path breaks in CI/production.  
- **Recommendation:** Use shared config (`DATA_DIR`/`BASE_DIR`) and remove machine-specific paths.

### CH-15 — Worker security: API key in query string + insecure dev fallback
- **Severity:** HIGH  
- **File:Line:** `packages/api/worker.js:~60-85`  
- **Description:** Accepts `api_key` via query params (leak risk). If `API_KEYS` binding missing, accepts any `bw_test_` key—dangerous if misconfigured in production.  
- **Recommendation:** Require `Authorization: Bearer` only; fail closed unless explicitly in development mode.

### CH-16 — Worker reliability: no timeouts/retries on upstream fetches
- **Severity:** HIGH  
- **File:Line:** `packages/api/worker.js:~35-310`  
- **Description:** No `AbortController` timeouts or retry/backoff; transient failures become cached partial/null data.  
- **Recommendation:** Add bounded retries with exponential backoff + jitter for 429/5xx and enforce 5–10s timeouts.

### CH-17 — Worker correctness: volume/size semantics inconsistent; combined VWAP weighting unreliable
- **Severity:** HIGH  
- **File:Line:** `packages/api/worker.js:~200-500`  
- **Description:** Uses `trade.count` as size for Kalshi (likely wrong), stores `vwap_volume` as “Dollar volume” but it’s raw size, then weights combined price by that. Produces biased combined prices.  
- **Recommendation:** Normalize to USD notional volume consistently; store both `contracts_volume` and `usd_volume`; verify Kalshi trade schema.

### CH-18 — Duplicate Worker implementations (drift risk) and cross-platform VWAP mixing
- **Severity:** HIGH / MEDIUM  
- **File:Line:** `packages/website/server/cloudflare-worker.js:~1-520`  
- **Description:** Duplicates API worker logic with different semantics; also mixes trades across platforms into one VWAP without normalization.  
- **Recommendation:** Extract shared module; compute per-platform VWAP + USD volume then combine.

---

## 3. MEDIUM FINDINGS (Grouped by Theme)

### A) Data integrity & identifier consistency
- **Index namespace mixing / mis-dedupe**
  - `pipeline_discover_markets_v2.py:~455-495` — mixes `cid`/`mid` against a single ID set; index schema unclear.  
  - **Fix:** Separate `polymarket_condition_ids`, `polymarket_slugs`, `kalshi_tickers`; version the index schema.
- **ID type mismatch causing reclassification churn**
  - `pipeline_refresh_political_tags.py:~300-340` — string vs int IDs cause “new tag” churn.  
  - **Fix:** Normalize IDs to string on load and write.
- **Market map selection ambiguity**
  - `generate_market_map.py:~214-240` — picks first match when multiple markets share ticker.  
  - **Fix:** choose highest-volume or include all matches with ranking.

### B) Error handling, exit codes, and observability
- **Failure returns success**
  - `pipeline_classify_kalshi_events.py:~175-185` — abort returns 0; orchestrator treats as success.  
  - **Fix:** non-zero exit on failure; `sys.exit(1)`.
- **Silent partial failures**
  - `pipeline_discover_markets_v2.py:~250-320` — non-200 tag fetches not logged (except 429).  
  - **Fix:** log status/text; retry 5xx; propagate failure markers.
- **Plot script robustness**
  - `brier_score_analysis.py:~170-240` — `ax` undefined if elections subset empty.  
  - **Fix:** early return or scope formatting inside conditional.

### C) Time handling & timezone correctness
- **Naive vs aware datetime comparisons**
  - `pull_trades_for_vwap.py:~232-236` — tz-naive parsed close_dt compared to tz-aware cutoff.  
  - **Fix:** parse with `utc=True` and standardize.
- **Election date parsing semantics**
  - `calculate_all_political_brier_scores.py:~70-110` — `.replace(tzinfo=utc)` can relabel rather than convert.  
  - **Fix:** explicit UTC parsing/localization rules.

### D) Concurrency, rate limiting, and performance
- **Global rate limiter serializes workers / wrong throughput**
  - `pull_polymarket_prices.py:~63-83`, `fetch_orderbooks.py:~41-88` — single lock limiter reduces throughput and mixes platforms.  
  - **Fix:** per-platform limiters; token bucket; correct comments.
- **Async rate limiting not thread-safe**
  - `enrich_markets_with_api_data.py:~70-92` — shared timestamp without lock allows bursts.  
  - **Fix:** `asyncio.Lock()` around rate limiting.
- **O(n^2) error attachment**
  - `enrich_markets_with_api_data.py:~625-640` — scan all errors per market.  
  - **Fix:** index errors by market_id.

### E) Schema contracts between scripts (producer/consumer drift)
- **Ticker prefix inconsistency**
  - `postprocess_tickers.py:~318-333` — drops `BWR-` prefix used elsewhere.  
  - **Fix:** define canonical ticker format and enforce across pipeline.
- **Unit of analysis drift (market vs contract)**
  - `calculate_all_political_brier_scores.py:~330-650`, `create_brier_cohorts.py:~35-120`, `generate_web_data.py:~520-740` — Polymarket emits Yes/No rows; downstream sometimes filters, sometimes not.  
  - **Fix:** choose market-level or contract-level and enforce consistently.

### F) Security & secrets hygiene
- **SMTP credentials stored in JSON; TLS context not explicit**
  - `logging_config.py:~245-285` — plaintext creds likely under logs; STARTTLS without explicit context.  
  - **Fix:** env/secrets manager; `ssl.create_default_context()`; validate TLS.
- **Test key file fallback**
  - `test_pipeline_components.py:~120-190` — reads `openai_api_key.txt`.  
  - **Fix:** require env var; ensure no plaintext key patterns.

---

## 4. LOW FINDINGS (Brief)

- `pipeline_daily_refresh.py:~135-165` — CLI parsing via `sys.argv.index` is fragile; use `argparse`.  
- `logging_config.py:~165-175` — log rotation glob overly broad.  
- `pipeline_classify_electoral.py:~35-45` — unused imports / inconsistent OpenAI client usage.  
- `audit_changelog.py:~140-155` — `record_gpt_call` ignores `script` argument.  
- `audit_gpt_logger.py:~130-150` — JSONL append lacks file locking (multi-process interleaving risk).  
- `calculate_liquidity_metrics.py:~40-120` — inconsistent std ddof conventions.  
- `build_tree.py:~140-160` — substring clustering heuristic may over-cluster.

---

## 5. ARCHITECTURE & DESIGN

**What’s working well**
- **Staged GPT classification pattern** (recall → verify → tiebreak) is a strong quality-control design and is consistently applied in key metadata pipelines.
- **Audit layer directionally solid**: validator/anomaly/changelog structures are clean and extensible; daily summary concept is valuable for governance.
- **Pragmatic fallbacks** exist in multiple places (e.g., Kalshi price extraction, enrichment lookup strategies), which is appropriate for messy external APIs.
- **Operational safety practices** like backup rotation before truncation are good.

**What should improve**
- **Shared contracts and utilities are missing**, leading to drift:
  - time cutoffs (election day, trading close),
  - atomic writes,
  - identifier normalization (`market_id`, `ticker`, `condition_id`, slug),
  - unit-of-analysis (market vs contract),
  - directory conventions (`DATA_DIR` vs `PAPER_DATA_DIR`).
- **Script composition is brittle**: many scripts assume implicit schemas and file locations; failures often degrade silently rather than failing fast.
- **Workers duplicate core pricing logic**, increasing drift risk and making correctness fixes harder to propagate.
- **Reliability controls are inconsistent**: some scripts have timeouts/retries; Workers largely do not; rate limiting is sometimes ineffective under concurrency.

---

## 6. PRIORITIZED ACTION ITEMS (Top 10)

1. **Implement atomic write utility and apply everywhere** (prices, trades, orderbooks, tickers, checkpoints).  
   Targets: `pull_*_prices.py`, `truncate_*`, `fetch_orderbooks.py`, `fetch_panel_a_trades.py`, `fix_name_collisions.py`, etc.

2. **Fix “full refresh” and incremental start-time logic** in `pull_polymarket_prices.py` and `pull_kalshi_prices.py`; add regression tests for flags.

3. **Fix election dates lookup schema + keying** in `pipeline_get_election_dates.py` (add `country`/`is_primary`, include `is_primary` in keys).

4. **Stabilize checkpoint identifiers across GPT pipelines**:  
   - `pipeline_classify_categories.py` checkpoint keyed by stable IDs,  
   - `pipeline_reclassify_incomplete.py` deterministic hashing + correct argument passing.

5. **Resolve directory/path contract mismatches** between paper outputs and `generate_web_data.py`; enforce a single output root and fail loudly when inputs are missing.

6. **Correct timezone handling for close times and cutoff computations**:  
   - `fetch_resolution_prices.py` UTC parsing,  
   - unify election-day truncation/VWAP cutoffs via shared helper.

7. **Fix Worker security posture**: remove query-string API keys; fail closed if `API_KEYS` missing; restrict CORS if browser use is intended.

8. **Add Worker timeouts + retries/backoff** for all upstream fetches; honor `Retry-After` and implement per-platform rate limiting.

9. **Normalize volume/size semantics and VWAP weighting** in `packages/api/worker.js` and `packages/website/server/cloudflare-worker.js`; store USD notional volume and combine platforms using consistent units.

10. **Standardize unit-of-analysis for accuracy/Brier pipelines** (market-level vs contract-level) and update downstream cohort/web/calibration scripts to match; add schema validation at load boundaries.

---
