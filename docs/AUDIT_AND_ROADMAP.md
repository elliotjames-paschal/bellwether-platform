# Bellwether Platform: Audit & Improvement Roadmap

**Last updated: 2026-03-17**
**Scope**: Bugs, scalability bottlenecks, OpenAI cost reduction, architecture improvements

---

## How to Use This Document

Work through sections in order. Each item has a checkbox, priority, and estimated effort. Items within a section are ordered by impact. Complete a section before moving to the next unless a critical item in a later section is blocking you.

**Priority key**: P0 = fix now, P1 = fix this week, P2 = fix this month, P3 = plan for next quarter

---

## 1. Critical Bugs (Fix Now)

### 1.1 Bare `except:` Clauses Swallowing Errors
- [ ] **P0 | 30 min** | `enrich_markets_with_api_data.py:340` — checkpoint loading catches all exceptions including KeyboardInterrupt
- [ ] **P0 | 15 min** | `config.py:189` — bare except in utility function
- [ ] **P0 | 15 min** | `truncate_kalshi_prices.py:220` — bare except
- [ ] **P0 | 15 min** | `truncate_polymarket_prices.py:223` — bare except
- **Fix**: Replace `except:` with `except (json.JSONDecodeError, IOError, OSError):` or similar specific exceptions. Log the error.

### 1.2 Silent Data Loss in create_tickers.py
- [ ] **P0 | 30 min** | `create_tickers.py:1056-1061` — if a GPT batch fails, entire batch is dropped from results with only a print statement. No retry, no record of which market_ids were lost.
- **Fix**: On batch failure, record failed market_ids in a separate list. After pipeline completes, log count and IDs of unprocessed markets. Optionally retry failed batches once.

### 1.3 KV Error Masking in API Worker
- [ ] **P1 | 30 min** | `worker-v2.js:60-64` — KV read failure returns `[]` (empty array), making it indistinguishable from "no markets exist". API returns 404 instead of 503.
- **Fix**: Return `null` on KV failure. In request handler, check for `null` and return `503 Service Unavailable` with a `Retry-After` header.

### 1.4 fillna(False) on Boolean Columns
- [ ] **P1 | 20 min** | `generate_web_data.py:1104-1108` — markets with unknown `is_closed` status silently become `False` (open), then get excluded from closed-market analysis.
- **Fix**: Drop rows with NaN `is_closed` explicitly and log count: `dropped = df[df['is_closed'].isna()]; log(f"Dropped {len(dropped)} markets with unknown close status")`

### 1.5 String/Int market_id Type Inconsistency
- [ ] **P1 | 45 min** | Throughout `generate_web_data.py` — market_id is sometimes int, sometimes string. Lookup failures silently produce MISC category.
- **Fix**: Add a single normalization step at data load time: `df['market_id'] = df['market_id'].astype(str)`. Do this once per DataFrame, remove scattered `.astype(str)` calls.

---

## 2. High-Priority Bugs

### 2.1 Plotly Memory Leak in Dashboard
- [ ] **P1 | 45 min** | `dashboard.js` — 40+ `Plotly.newPlot()` calls without `Plotly.purge()`. Memory grows as users switch tabs/charts.
- **Fix**: Before every `Plotly.newPlot(id, ...)`, add `Plotly.purge(id);`. Create a helper:
  ```javascript
  function safePlot(id, data, layout, config) {
    Plotly.purge(id);
    Plotly.newPlot(id, data, layout, config);
  }
  ```

### 2.2 No Input Validation in API Worker
- [ ] **P1 | 30 min** | `worker-v2.js:889-932` — tokenId, pm_token, k_ticker have no length or format validation.
- **Fix**: Add validation: `if (!tokenId || tokenId.length > 100 || !/^[a-zA-Z0-9\-_.]+$/.test(tokenId)) return 400;`

### 2.3 Globe CDN Fetch With No Timeout
- [ ] **P2 | 20 min** | `globe-hero.js:1032-1037` — `fetch('cdn.jsdelivr.net/...')` has no timeout. Globe hangs if CDN is unreachable.
- **Fix**: Use AbortController with 8-second timeout. Add fallback to bundled low-res topology.

### 2.4 safe_round() Converts NaN to 0
- [ ] **P2 | 20 min** | `generate_web_data.py:615-623` — All NaN/Infinity values become 0, making it impossible to distinguish missing data from actual zeros.
- **Fix**: Return `None` for NaN/missing. Use `allow_nan=False` in `json.dump()` (already done) — this will catch any remaining NaN propagation.

### 2.5 Global Mutable Cache With No Invalidation
- [ ] **P2 | 15 min** | `generate_web_data.py:53-54` — `_ticker_data_cache` persists for entire process lifetime.
- **Fix**: Not a problem for single-run scripts, but add a `clear_caches()` function for testing. Document that caches are process-scoped.

### 2.6 Dashboard Null Coalescing Missing
- [ ] **P2 | 30 min** | `dashboard.js:199-220` — accessing `stats.shared_elections.combined` without checking `shared_elections` exists.
- **Fix**: Use optional chaining: `stats?.shared_elections?.combined?.accuracy`

---

## 3. Medium-Priority Bugs

- [ ] **P2 | 15 min** | `postprocess_tickers.py:44-93` — Date regex accepts invalid dates (Feb 31). Add month-day validation.
- [ ] **P2 | 20 min** | `dashboard.js:949-1071` — Volume chart race condition when user switches categories during load. Track request version.
- [ ] **P2 | 20 min** | `mcp/src/api/client.ts:37-43` — No fetch timeout. Add 5-second AbortController.
- [ ] **P2 | 15 min** | `pipeline_daily_refresh.py:156-157` — Pipeline state load errors silently ignored. Log warning.
- [ ] **P3 | 15 min** | `generate_web_data.py:1105-1108` — `== True` comparison on columns that may contain string 'True'. Use `.astype(bool)` first.

---

## 4. OpenAI API Cost Reduction

### Current GPT Usage (Daily Pipeline)

| Script | Model | Calls/Run | What It Does |
|--------|-------|-----------|-------------|
| `pipeline_classify_categories.py` | gpt-4o | 3 stages x N/50 batches | Classify new markets into 16 categories |
| `pipeline_classify_electoral.py` | gpt-4o | 3 stages x N/20 batches | Extract electoral details (country, office, party) |
| `pipeline_get_election_dates.py` | gpt-4o | 3 stages x N/50 batches | Look up election dates |
| `pipeline_refresh_political_tags.py` | gpt-4o | N/500 batches | Tag new Polymarket slugs as political/not |
| `pipeline_reclassify_incomplete.py` | gpt-4o | ~N individual | Fix incomplete classifications |
| `pipeline_select_election_winners.py` | **gpt-4o-search-preview** | ~N individual | Web search for election results |
| `pipeline_compare_resolutions.py` | gpt-4o | ~N individual | Compare resolution outcomes |
| `create_tickers.py` | gpt-4o | N/50 batches | Generate BWR canonical tickers |

**Key observation**: Most scripts process only NEW markets daily (not all 50K). Typical daily new market count: 50-200. But `create_tickers.py` and full refreshes reprocess thousands.

### 4.1 Switch to gpt-4o-mini Where Possible
- [ ] **P1 | 30 min | Est. savings: 60-80% on affected scripts**
- `pipeline_classify_categories.py` — Category classification is straightforward (16 fixed categories, clear descriptions). gpt-4o-mini handles this well.
- `pipeline_refresh_political_tags.py` — Binary "is this political?" classification. Trivial for mini.
- `pipeline_get_election_dates.py` — Date extraction from well-structured text. Mini-capable.
- **Keep gpt-4o for**: `create_tickers.py` (complex multi-field extraction), `pipeline_classify_electoral.py` (nuanced country/office/party), `pipeline_select_election_winners.py` (web search).
- **How**: Change `MODEL = "gpt-4o"` to `MODEL = "gpt-4o-mini"` in each script. Run validation against 100 known-good outputs to verify quality.

### 4.2 Increase Batch Sizes
- [ ] **P1 | 15 min | Est. savings: 20-30% fewer API calls**
- `pipeline_classify_electoral.py`: BATCH_SIZE = 20 → 50 (same as categories). Each GPT call classifies more markets.
- `create_tickers.py`: default batch_size = 50 → 100 (test quality at larger batch).
- **Trade-off**: Larger batches = more tokens per call = slightly more cost per call, but fewer calls overall. Net savings because per-call overhead (system prompt resent each time) is amortized.

### 4.3 Skip Already-Classified Markets
- [ ] **P1 | 45 min | Est. savings: 50-90% on full refreshes**
- Currently, `--full-refresh` reclassifies ALL markets, not just new ones.
- **Fix**: Before sending to GPT, check if market already has a valid classification/ticker. Only send unclassified or flagged-for-reclassification markets.
- Add a `--force-reclassify` flag for when you actually want to redo everything.
- This is the **single biggest cost saver** for full refresh runs.

### 4.4 Use OpenAI Batch API (Async)
- [ ] **P2 | 2-3 hours | Est. savings: 50% cost reduction**
- OpenAI's [Batch API](https://platform.openai.com/docs/guides/batch) processes requests asynchronously at **50% cost** (as of 2025).
- Best candidates: `create_tickers.py` (large batch runs, not time-sensitive), `pipeline_classify_electoral.py` during full refresh.
- **How it works**: Upload a JSONL file of requests → OpenAI processes within 24h → Download results.
- **Trade-off**: Results not immediate. Only usable for non-time-critical pipeline steps (not daily incremental).
- **Implementation**: Create a `batch_mode` flag. When set, write requests to JSONL, upload via Batch API, poll for completion, parse results.

### 4.5 Cache GPT Responses
- [ ] **P2 | 1 hour | Est. savings: variable, prevents duplicate work**
- If pipeline crashes mid-run and restarts, markets already classified get re-sent to GPT.
- **Fix**: `create_tickers.py` already has checkpoint logic. Ensure all GPT scripts save intermediate results after each batch (not just at end).
- `pipeline_classify_categories.py`, `pipeline_classify_electoral.py`: Add checkpoint save after each batch completes.

### 4.6 Replace GPT With Rules Where Possible
- [ ] **P3 | 2-4 hours | Est. savings: eliminates calls for rule-matched markets**
- `pipeline_refresh_political_tags.py` — Many Polymarket slugs are obviously political (contain "election", "president", "congress") or obviously not ("bitcoin", "nba", "weather"). A keyword filter could handle 70%+ without GPT.
- `pipeline_get_election_dates.py` — US elections have known fixed dates (first Tuesday after first Monday in November). Only international/special elections need GPT lookup.
- `postprocess_tickers.py` already does this well (16+ rule-based fixes before any GPT call).
- **How**: Add a pre-filter that classifies obvious cases with rules, only sends ambiguous cases to GPT.

### 4.7 Monitor and Cap Spending
- [ ] **P2 | 30 min**
- Add a cost tracker to `audit/audit_gpt_logger.py` (already exists but may not be wired up to all scripts).
- Log tokens used per script per run. Set a daily budget alert.
- **Quick win**: `export OPENAI_LOG=info` to see token counts in pipeline logs.

### Cost Savings Summary

| Action | Effort | Savings | Priority |
|--------|--------|---------|----------|
| Switch 3 scripts to gpt-4o-mini | 30 min | 60-80% on those scripts | P1 |
| Skip already-classified on full refresh | 45 min | 50-90% on full refresh | P1 |
| Increase batch sizes | 15 min | 20-30% fewer calls | P1 |
| OpenAI Batch API for create_tickers | 2-3 hours | 50% on batch runs | P2 |
| Checkpoint all GPT scripts | 1 hour | Prevents duplicate work | P2 |
| Rule-based pre-filtering | 2-4 hours | Eliminates easy cases | P3 |

---

## 5. Scalability: Data Storage

### 5.1 Replace Flat Files With SQLite (Critical Path)
- [ ] **P1 | 1-2 days | Unblocks growth past 60K markets**
- `enriched_political_markets.json` (759MB for 21K markets) is loaded fully into memory by multiple scripts. At 60K markets it exceeds server RAM.
- **Phase 1**: Create SQLite database with tables for `markets`, `prices`, `tickers`, `enrichments`. Migrate enriched_political_markets.json.
- **Phase 2**: Update `enrich_markets_with_api_data.py` to read/write SQLite instead of JSON.
- **Phase 3**: Update downstream scripts to query SQLite (with indexes on market_id, category, platform).
- **Keep master CSV as export format** for compatibility, but generate it from SQLite.

### 5.2 Chunked CSV Reading
- [ ] **P1 | 1 hour | Drops peak memory 80%**
- Replace `pd.read_csv(low_memory=False)` with `pd.read_csv(chunksize=10000)` in memory-heavy scripts.
- Most impactful in: `generate_web_data.py`, `enrich_markets_with_api_data.py`, `calculate_liquidity_metrics.py`.
- **Quick pattern**:
  ```python
  chunks = []
  for chunk in pd.read_csv(file, chunksize=10000):
      chunks.append(chunk[chunk['platform'] == 'Polymarket'])  # filter early
  df = pd.concat(chunks)
  ```

### 5.3 Consolidate Overlapping Data Files
- [ ] **P2 | 4-8 hours**
- Current: 6 different representations of market data (master CSV, enriched JSON, tickers JSON, market_map, active_markets, monitor_data). ~50% redundancy.
- **Target**: Single source of truth (SQLite) with views/exports for each consumer.
- Start by documenting which fields each consumer actually uses, then design minimal export formats.

### 5.4 Archive Old Price Data
- [ ] **P2 | 2 hours**
- `kalshi_all_political_prices_CORRECTED_v3.json` (455MB) — historical prices rarely queried.
- Move to compressed Parquet or SQLite. Load on-demand for analysis scripts, not pipeline.

---

## 6. Scalability: Pipeline Performance

### 6.1 Parallelize Analysis Phase
- [ ] **P1 | 2-3 hours | Saves ~60 min per pipeline run**
- 18 analysis scripts run sequentially in Phase 4. Many are independent (Brier by category, calibration, platform comparison).
- **Fix**: Use `concurrent.futures.ProcessPoolExecutor` in `pipeline_daily_refresh.py` for independent scripts.
- Group into: {Brier-dependent scripts (need accuracy CSV)} and {independent scripts (volume, distribution, etc.)}.

### 6.2 Incremental Processing
- [ ] **P2 | 4-8 hours**
- Most scripts rewrite entire output files even when only 50 new markets were added.
- **Fix**: Track "last processed market_id" per script. Only process new markets. Merge into existing output.
- Start with `create_tickers.py` (already has `--only-new` flag) and `enrich_markets_with_api_data.py` (has checkpoint logic).

### 6.3 Per-Phase Timing & Memory Logging
- [ ] **P2 | 1 hour**
- `pipeline_daily_refresh.py` doesn't log per-phase duration or peak memory.
- **Fix**: Add `time.time()` around each phase. Log `resource.getrusage(resource.RUSAGE_SELF).ru_maxrss` after each script.
- Enables early warning when scripts approach timeout or OOM.

### 6.4 Increase Script Timeout
- [ ] **P2 | 10 min**
- Current: 1800 seconds (30 min) per script. `enrich_markets_with_api_data.py` and `create_tickers.py` can exceed this at scale.
- **Fix**: Increase to 3600 (1 hour) for heavy scripts, keep 1800 for lightweight ones.

---

## 7. Scalability: API & Frontend

### 7.1 Paginate active_markets.json
- [ ] **P2 | 4-8 hours**
- Currently 35MB loaded entirely into Cloudflare Worker memory and browser.
- **Fix**: Split into paginated KV entries (`active_markets:page:1`, `active_markets:page:2`, etc.). Worker loads pages on-demand based on query.
- Or: Serve pre-filtered category-specific files (`active_markets:ELEC`, `active_markets:MONETARY`, etc.).

### 7.2 Compress Static Data Files
- [ ] **P1 | 30 min**
- JSON files in `docs/data/` are served uncompressed via GitHub Pages.
- **Fix**: GitHub Pages supports gzip via CloudFlare. Verify `Content-Encoding: gzip` is set. If not, pre-compress files and serve `.json.gz` with proper headers.

### 7.3 Lazy Load Dashboard Charts
- [ ] **P3 | 2-4 hours**
- All 40+ JSON files fetched on page load. Only visible tab's data needed initially.
- **Fix**: Fetch data on tab switch, not page load. Cache fetched data in memory.

---

## 8. Infrastructure

### 8.1 Server RAM
- [ ] **P1 | Upgrade when budget allows**
- Current: 3.7GB total, 29MB free during pipeline. One large script = OOM.
- **Minimum target**: 8GB. Gives headroom for 2x market growth.
- **Alternative**: Implement SQLite + chunked reads first (sections 5.1, 5.2) to reduce memory pressure without hardware upgrade.

### 8.2 Pipeline Overlap Prevention
- [ ] **P2 | 30 min**
- Lock file exists but no alerting if a run is skipped due to overlap.
- **Fix**: If lock file exists and is >6 hours old, send alert (email/webhook) and consider stale.

### 8.3 Monitoring & Alerting
- [ ] **P2 | 2-4 hours**
- No alerts for: pipeline failure, OOM kills, stale data (>24h since last update), GPT budget exceeded.
- **Fix**: Add a simple health check endpoint to the worker that returns `last_updated` timestamp from KV. External monitor (UptimeRobot, free tier) pings it every 30 min.

---

## 9. Category System Cleanup (Deferred)

These items are tracked but not urgent since the research tab migration is done and API/MCP use separate data paths.

- [ ] **P3** | Update `worker-v2.js` VALID_CATEGORIES to new codes
- [ ] **P3** | Update `packages/mcp/src/types.ts` VALID_CATEGORIES to new codes
- [ ] **P3** | Update `generate_worker_index.py` to normalize categories
- [ ] **P3** | Update `docs/docs.html` API documentation with new category codes
- [ ] **P3** | Remove `extractCategory()` function in worker (no longer needed once data uses new codes)

---

## Progress Tracking

| Section | Items | Completed | Last Updated |
|---------|-------|-----------|-------------|
| 1. Critical Bugs | 5 | 0 | 2026-03-17 |
| 2. High-Priority Bugs | 6 | 0 | 2026-03-17 |
| 3. Medium Bugs | 5 | 0 | 2026-03-17 |
| 4. OpenAI Cost | 7 | 0 | 2026-03-17 |
| 5. Data Storage | 4 | 0 | 2026-03-17 |
| 6. Pipeline Perf | 4 | 0 | 2026-03-17 |
| 7. API/Frontend | 3 | 0 | 2026-03-17 |
| 8. Infrastructure | 3 | 0 | 2026-03-17 |
| 9. Category Cleanup | 5 | 0 | 2026-03-17 |
