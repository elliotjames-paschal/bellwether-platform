# Bellwether Human Feedback Mechanism

## Overview

The feedback mechanism creates a closed loop between human reviewers and the automated ticker pipeline. Reviewers label cross-platform market pairs via the website; those labels flow through four pipeline steps that evaluate accuracy, apply corrections, and generate rules for the next run.

```
Website feedback form
        │
        ▼
┌─────────────────────────┐
│  pipeline_ingest_        │  Step 1: Parse Google Sheet rows → human_labels.json
│  feedback.py             │
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│  pipeline_apply_         │  Step 2: Apply labels as ground-truth overrides
│  human_labels.py         │
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│  pipeline_evaluate_      │  Step 3: Compute precision/recall, generate suggested labels
│  matches.py              │
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│  generate_ticker_        │  Step 4: Extract error patterns → correction & disambiguation rules
│  corrections.py          │
└─────────────────────────┘
```

---

## Data Flow

### File Dependencies

| File | Created By | Consumed By |
|------|-----------|-------------|
| `human_labels.json` | pipeline_ingest_feedback.py | pipeline_apply_human_labels.py, pipeline_evaluate_matches.py, generate_ticker_corrections.py |
| `tickers_postprocessed.json` | postprocess_tickers.py | All four feedback scripts (read); pipeline_apply_human_labels.py (write) |
| `match_accuracy_report.json` | pipeline_evaluate_matches.py | generate_ticker_corrections.py |
| `ticker_corrections.json` | generate_ticker_corrections.py | postprocess_tickers.py (NEXT run, Fix 15) |
| `ticker_disambiguations.json` | generate_ticker_corrections.py | postprocess_tickers.py (NEXT run, Fix 16) |
| `match_exclusions.json` | pipeline_apply_human_labels.py | pipeline_apply_human_labels.py (read on next run to avoid duplicates) |
| `near_matches.json` | pipeline_apply_human_labels.py | Website display |
| `cross_platform_reviewed_pairs.json` | pipeline_apply_human_labels.py | pipeline_discover_cross_platform.py (skip re-surfacing) |
| `cross_platform_candidates.json` | pipeline_discover_cross_platform.py | pipeline_evaluate_matches.py (suggested labels) |
| `cross_platform_resolution_verdicts.json` | pipeline_compare_resolutions.py | pipeline_evaluate_matches.py (suggested labels) |
| `combined_political_markets_with_electoral_details_UPDATED.csv` | Earlier pipeline phases | pipeline_apply_human_labels.py (R+W), pipeline_evaluate_matches.py (R) |

All paths resolve to `data/` via `config.DATA_DIR`.

---

## Batch ID Traceability

All feedback pipeline steps share a single `batch_id` per run for traceability:

- **Manual runs:** Each script auto-generates a batch ID in format `batch_YYYYMMDD_HHMMSS` if `--batch-id` is not provided.
- **Orchestrated runs:** `pipeline_daily_refresh.py` generates one `feedback_batch_id` and passes it via `--batch-id` to all four steps, so every label, exclusion, and correction from a single run shares the same ID.
- The batch ID appears as `ingested_batch_id` on labels (Step 1), `applied_batch_id` on applied labels (Step 2), and `batch_id` on exclusion entries and correction rules.

---

## Step 1: Ingest Feedback

**Script:** `pipeline_ingest_feedback.py`

**Source:** Published Google Sheet CSV at:
```
https://docs.google.com/spreadsheets/d/e/2PACX-1vRPiDl8J5hruzzB3_CR83cDz1xrVob9XAgZn_cyfulKX4e3oBGmSbUvP_Ax4hSoesSDoDJXffWtqvjI/pub?output=csv
```

**Behavior:**
1. Fetches all rows from the Google Sheet (or reads `--csv-file` if provided)
2. Skips rows with timestamps ≤ `last_ingested_timestamp` in `human_labels.json`
3. Parses feedback type from the `Feedback Type` column
4. Resolves BWR ticker keys to `market_id` values using `tickers_postprocessed.json`
5. Appends new labels to `human_labels.json` with status `"pending"`

**Default cutoff:** `DEFAULT_LAST_INGESTED = "2026-02-12T22:41:28.460Z"` — all rows before this were already reviewed and applied manually during initial development, so they are skipped on first run.

**Label Types:**

| Feedback Type (CSV) | Label Type (JSON) | Meaning |
|---------------------|-------------------|---------|
| `same-event` + same-rules | `same_event_same_rules` | Markets are identical across platforms |
| `same-event` + different-rules | `same_event_different_rules` | Same event, different resolution criteria |
| `different-event` | `different_event` | Markets incorrectly matched — different events |
| `not-political` | `not_political` | Market is not about politics |
| `wrong-category` | `wrong_category` | Market has wrong political category |
| `other` | `other` | Freeform issue |

**Label Structure:**
```json
{
    "label_id": "hl_a1b2c3d4e5f6",
    "source": "google_sheet",
    "ingested_at": "2026-03-10T08:00:00.000Z",
    "original_timestamp": "2026-03-10T07:45:00.000Z",
    "label_type": "same_event_same_rules",
    "market_ids": ["kalshi_market_123", "poly_market_456"],
    "market_keys": ["BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028"],
    "platforms": ["Kalshi", "Polymarket"],
    "description": "Same market, mechanism differs",
    "status": "pending",
    "applied_at": null,
    "applied_action": null,
    "ingested_batch_id": "batch_20260310_080000"
}
```

**CLI:**
```bash
python pipeline_ingest_feedback.py --dry-run                          # Preview without writing
python pipeline_ingest_feedback.py --csv-file /tmp/f.csv              # Read from local file
python pipeline_ingest_feedback.py --batch-id batch_20260310_manual   # Explicit batch ID
```

**Idempotency:** Running ingest twice produces 0 new labels on the second run (timestamp-based deduplication).

---

## Step 2: Apply Human Labels

**Script:** `pipeline_apply_human_labels.py`

**Files read/written:**
- `human_labels.json` (R+W) — reads pending labels, updates status
- `tickers_postprocessed.json` (R+W) — modifies tickers for same-event merges
- `match_exclusions.json` (R+W) — exclusion entries for different-event pairs
- `near_matches.json` (R+W) — near-match entries for same-event-different-rules
- `cross_platform_reviewed_pairs.json` (R+W) — prevents re-discovery of reviewed pairs
- Master CSV (R+W) — category changes for not-political/wrong-category

### _SPLIT Migration

On startup (before processing any labels), the script runs `migrate_split_tickers()`. This converts legacy `_SPLIT` suffixes (an older mechanism for breaking incorrect matches) into the current `match_exclusions.json` approach:

1. Scans all tickers for strings ending in `_SPLIT`
2. Strips the suffix and finds other markets sharing the base ticker
3. Creates exclusion entries with `reason: "migrated_from_split"` and `batch_id: "migration"`

This migration is idempotent — once all `_SPLIT` suffixes are converted, subsequent runs find nothing to migrate.

### Behavior by label type

#### `same_event_same_rules`
- Validates that the two tickers share the same agent, action, target, and timeframe
- If only mechanism/threshold differ: unifies to a single ticker in `tickers_postprocessed.json`
- Sets `match_source: "human"` and `human_label_id` on the unified entry
- If core identity fields differ: marks label `status: "needs_review"` (not auto-applied)

#### `same_event_different_rules`
- Adds the pair to `near_matches.json` for website display
- **Cross-platform pairs only:** near-match entries are only created when the two markets are on different platforms. Same-platform pairs are silently skipped (label is still marked `applied` but no near-match file entry is created).
- These pairs are shown as "same event, different resolution" — not merged

#### `different_event`
- If the two markets already have different tickers: marks `applied_action: "already_different"` — no file changes
- If the two markets share a ticker: creates pairwise exclusion entries in `match_exclusions.json` with `reason: "different_event"`
- Always adds entries to `cross_platform_reviewed_pairs.json` with `verdict: "DIFFERENT"` and `match_source: "human"` (for all cross-platform pairs)

**Exclusion entry structure:**
```json
{
    "exclusion_id": "sha256_of_sorted_pair",
    "market_id_a": "kalshi_market_123",
    "market_id_b": "poly_market_456",
    "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
    "reason": "different_event",
    "source_label_id": "hl_a1b2c3d4e5f6",
    "created_at": "2026-03-10T08:00:00.000Z",
    "batch_id": "batch_20260310_080000"
}
```

#### `not_political`
- Sets category to `"16. NOT_POLITICAL"` in the master CSV

#### `wrong_category`
- Attempts to parse the correct category from the description
- If a valid category name is found: recategorizes in the master CSV
- Otherwise: marks `status: "needs_review"`

**Status transitions:** `pending` → `applied` or `pending` → `needs_review`

**CLI:**
```bash
python pipeline_apply_human_labels.py --dry-run                          # Preview changes
python pipeline_apply_human_labels.py                                     # Apply for real
python pipeline_apply_human_labels.py --batch-id batch_20260310_manual   # Explicit batch ID
```

---

## Step 3: Evaluate Match Accuracy

**Script:** `pipeline_evaluate_matches.py`

**Metrics computed:**

| Metric | Definition |
|--------|-----------|
| True Positive | Human says "same event" AND pipeline assigned same ticker |
| False Negative | Human says "same event" BUT pipeline assigned different tickers |
| False Positive | Human says "different event" BUT pipeline assigned same ticker |
| True Negative | Human says "different event" AND pipeline assigned different tickers |
| Precision | TP / (TP + FP) |
| Recall | TP / (TP + FN) |
| F1 | Harmonic mean of precision and recall |

### Suggested Labels

The script also outputs a ranked list of market pairs most valuable to label next, written to the `suggested_labels` field in `match_accuracy_report.json`.

**Scoring formula:**
```
priority = 0.4 × uncertainty
         + 0.3 × cosine_similarity
         + 0.2 × novelty
         + 0.1 × log(volume)
```

| Signal | Weight | What It Measures |
|--------|--------|-----------------|
| `uncertainty` | 0.4 | Ticker component similarity. Pairs with 0 core diffs but different mechanism/threshold score 0.9 (highest). Pairs with 1 core diff score 0.7. 3+ core diffs score 0.15. |
| `cosine_similarity` | 0.3 | Sentence-transformer embedding similarity from cross-platform discovery pipeline |
| `novelty` | 0.2 | `1 / (1 + existing_labels_for_this_pattern)` — new error patterns get priority; saturated patterns deprioritized |
| `log_volume` | 0.1 | `log10(combined_volume) / 8`, capped at 1.0 — tiebreaker only, log-scaled to prevent high-volume dominance |

**Design rationale:**
- Uncertainty-first ranking surfaces pairs where the pipeline is most confused, not just popular markets
- Novelty scoring ensures error-pattern diversity — once a diff pattern has enough labels, the system explores new patterns
- Volume is log-scaled so $50M markets don't drown out $500K ones, but between two equally uncertain pairs the higher-impact one ranks first
- Already-labeled pairs are excluded from suggestions

**CLI:**
```bash
python pipeline_evaluate_matches.py --verbose              # Print disagreements + suggested labels
python pipeline_evaluate_matches.py --max-suggestions 50   # More suggestions
python pipeline_evaluate_matches.py --batch-id batch_xyz   # Explicit batch ID
```

**Output structure:**
```json
{
    "suggested_labels": [
        {
            "rank": 1,
            "ticker_a": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
            "ticker_b": "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
            "kalshi_question": "Will Trump win the 2028 presidential election?",
            "poly_question": "Trump wins 2028 presidency?",
            "combined_volume": 52000000,
            "cosine_similarity": 0.89,
            "score": 0.8234,
            "score_components": {
                "uncertainty": 0.9,
                "cosine_similarity": 0.89,
                "novelty": 1.0,
                "log_volume": 0.71
            },
            "verdict": "OVERLAPPING",
            "bucket": "B",
            "reason": "Tickers nearly match — likely same event with different resolution; Novel error pattern (no existing labels)"
        }
    ],
    "suggested_labels_scoring": {
        "formula": "0.4*uncertainty + 0.3*cosine_similarity + 0.2*novelty + 0.1*log_volume",
        "weights": { "uncertainty": 0.4, "cosine_similarity": 0.3, "novelty": 0.2, "log_volume": 0.1 }
    }
}
```

---

## Step 4: Generate Ticker Corrections & Disambiguations

**Script:** `generate_ticker_corrections.py`

This step produces **two** output files: correction rules (alias mappings for false negatives) and disambiguation rules (re-extraction triggers for false positives).

### Correction Rules (ticker_corrections.json)

**Behavior:**
1. Reads `match_accuracy_report.json` for false negatives (pairs humans say match but the pipeline didn't)
2. Classifies what differs between the two tickers (mechanism, agent, target, timeframe)
3. Counts how often each specific diff appears (e.g., PROJECTED→CERTIFIED seen 5 times)
4. If frequency meets the per-type threshold, generates a correction rule

**Per-type minimum frequency thresholds:**

| Correction Type | Default Threshold | Risk Level | Rationale |
|----------------|-------------------|------------|-----------|
| `mechanism_alias` | 2 | Low | Resolution method change (PROJECTED→CERTIFIED). Cannot merge different people/events. |
| `timeframe_alias` | 2 | Low | Date formatting difference (2026 vs 2026_Q3). Underlying event is the same. |
| `target_alias` | 4 | Medium-high | Could merge different offices (GOV_GA vs SEN_GA) if alias is wrong. |
| `agent_alias` | 5 | High | Could merge different people (C_POWELL vs J_POWELL). Needs strong consensus. |

**Correction output:**
```json
{
    "type": "mechanism_alias",
    "from": "PROJECTED",
    "to": "CERTIFIED",
    "frequency": 5,
    "source_labels": ["hl_a1b2c3", "hl_d4e5f6"]
}
```

### Disambiguation Rules (ticker_disambiguations.json)

**Behavior:**
1. Reads `match_accuracy_report.json` for false positives (pairs humans say differ but the pipeline merged)
2. Analyzes the shared ticker to identify which field(s) were over-collapsed using `identify_collapsed_fields()` — supports multi-field detection
3. Groups false positives by (collapsed_field, agent, action, target) pattern
4. If frequency meets the per-type threshold, generates a disambiguation rule
5. False positives with no detectable collapse ("Category C") are counted as unresolvable — these remain pair-specific exclusions only

### Three categories of false positives

| Category | Cause | Fix Strategy |
|----------|-------|-------------|
| A: Field over-collapse | One field too generic (ANY threshold, bare year, STD mechanism) | Re-extract from source text → disambiguation rule |
| B: Name/entity collision | Two different entities mapped to same string (POWELL → Jerome vs Colin) | Re-run NAME_COLLISIONS lookup → disambiguation rule |
| C: Genuinely identical tickers | Ticker is correct for both markets, events are just different | Pair-specific exclusion only (no rule possible) |

**Disambiguation types and thresholds:**

| Rule Type | Trigger | Action | Default Threshold |
|-----------|---------|--------|-------------------|
| `threshold_disambiguation` | `threshold == "ANY"` | Re-extract threshold from question text | 2 |
| `timeframe_disambiguation` | Bare 4-digit year timeframe (e.g., `"2026"`) | Re-extract monthly/quarterly from description | 2 |
| `mechanism_disambiguation` | `mechanism == "STD"` (generic fallback) | Infer specific mechanism from question keywords | 2 |
| `agent_disambiguation` | Agent is bare last name in `NAME_COLLISIONS` | Re-run first-name lookup from question text | 5 |
| `target_disambiguation` | Target is in `_AMBIGUOUS_TARGETS` (e.g., SENATE, RATE) | Append state/qualifier from question text | 4 |

Agent and target disambiguation rules are primarily **diagnostic** — they attempt re-extraction but their main value is surfacing patterns that need developer attention (e.g., adding entries to `NAME_COLLISIONS`).

**Disambiguation output:**
```json
{
    "type": "threshold_disambiguation",
    "action": "re_extract_threshold",
    "pattern": {
        "agent": "TRUMP",
        "action": "WIN",
        "target": "PRES_US",
        "threshold": "ANY"
    },
    "frequency": 3,
    "source_labels": ["hl_x1y2z3", "hl_a4b5c6", "hl_d7e8f9"]
}
```

The output file also includes an `unresolvable_count` field showing how many false positives had no detectable field-level collapse.

### How corrections and disambiguations are consumed

On the NEXT pipeline run, `postprocess_tickers.py` applies both:

- **Fix 15 (Corrections):** Reads `ticker_corrections.json`. For each ticker, checks all correction rules. If a ticker's field value matches a rule's `from` value, replaces it with the `to` value. All matching rules are applied (not just the first match).

- **Fix 16 (Disambiguations):** Reads `ticker_disambiguations.json`. For each ticker, checks if it matches a rule's `pattern` (agent, action, target must all match). Then by rule type:
  - `threshold_disambiguation`: Re-extracts threshold from `original_question` using `extract_threshold()`
  - `timeframe_disambiguation`: Re-extracts monthly/quarterly from description using `extract_date_from_description()`
  - `mechanism_disambiguation`: Infers specific mechanism from question keywords (CERTIFIED, PROJECTED, etc.) using `infer_specific_mechanism()`
  - `agent_disambiguation`: Re-runs `NAME_COLLISIONS` lookup to disambiguate bare last names
  - `target_disambiguation`: Appends state/office qualifiers from question text using `infer_specific_target()`

This closes the feedback loop — false negatives produce alias rules that merge tickers; false positives produce disambiguation rules that split them.

**CLI:**
```bash
python generate_ticker_corrections.py --dry-run                                    # Preview
python generate_ticker_corrections.py --min-frequency 3                            # Uniform threshold
python generate_ticker_corrections.py --min-freq-agent 3 --min-freq-mechanism 1    # Per-type overrides
python generate_ticker_corrections.py --min-freq-disamb-agent 3                    # Disambiguation-specific
python generate_ticker_corrections.py --batch-id batch_xyz                         # Explicit batch ID
```

---

## Orchestration

**Script:** `pipeline_daily_refresh.py`

The feedback steps run as part of Phase 5 (web data generation):

```bash
python pipeline_daily_refresh.py --start-phase 5
```

**Key behaviors:**
- Generates a single `feedback_batch_id` (format `batch_YYYYMMDD_HHMMSS`) and passes it to all four feedback steps via `--batch-id`
- All four feedback steps are marked `required=False` — if any step fails, the orchestrator logs the error and continues to the next step. This prevents feedback pipeline issues from blocking the main data refresh.

---

## Website Integration

### In-Modal Feedback (Single Market)

Each market detail modal in the Dispersion tab includes a feedback section at the bottom. This allows users to submit feedback while viewing a specific market pair:

- **Same Event (Cross-Platform Match)** — with sub-options:
  - Same Rules (resolution criteria identical)
  - Different Rules (same event, different resolution)
- **Different Events** — markets incorrectly matched
- **Not Political** — market isn't about politics
- **Wrong Category** — mislabeled political category
- **Other Issue** — freeform

The in-modal form submits feedback for the **single market** being viewed. The modal also displays verbatim Kalshi and Polymarket titles in the platform link boxes for easy comparison.

### Sidebar Review Mode (Multiple Markets)

The sidebar "Help Us Match Markets" card activates review mode:
1. Checkboxes appear on all market cards
2. User selects 2+ markets and clicks "Submit Feedback"
3. Feedback modal opens with the same label options
4. Submission includes **all selected market keys** with platform and category metadata

### Submission Path

Submissions go to the Google Sheet via Apps Script webhook and are also stored in `localStorage.marketFeedback` as backup.

---

## Testing the Feedback Loop

### Prerequisites

These files must exist in `data/`:
- `tickers_postprocessed.json` (from a prior pipeline run with GPT ticker generation)
- `combined_political_markets_with_electoral_details_UPDATED.csv` (master CSV)
- `enriched_political_markets.json.gz` (enriched market data)

### Step-by-step

```bash
cd packages/pipelines

# Step 1: Ingest — creates data/human_labels.json
python pipeline_ingest_feedback.py --dry-run           # Preview
python pipeline_ingest_feedback.py                     # Run for real

# Step 2: Apply — modifies tickers, creates exclusions and near-matches
python pipeline_apply_human_labels.py --dry-run        # Preview
python pipeline_apply_human_labels.py                  # Apply

# Step 3: Evaluate — creates data/match_accuracy_report.json
python pipeline_evaluate_matches.py --verbose

# Step 4: Corrections — creates data/ticker_corrections.json + ticker_disambiguations.json
python generate_ticker_corrections.py --dry-run        # Preview
python generate_ticker_corrections.py                  # Write
```

### What to verify

1. **Ingestion**: Only rows after `DEFAULT_LAST_INGESTED` (or `last_ingested_timestamp`) appear. Run twice — second run adds 0 labels.
2. **_SPLIT migration**: Any legacy `_SPLIT` tickers are converted to exclusion entries on first run.
3. **Apply**: `same_event_same_rules` labels where agent/action/target differ should show `status: "needs_review"`, not `"applied"`.
4. **Exclusions**: `different_event` labels produce entries in `match_exclusions.json` (when tickers were shared).
5. **Evaluate**: Precision/recall numbers are plausible. Suggested labels rank uncertain pairs above high-volume-but-obvious ones.
6. **Corrections**: Rules match expected alias patterns. `mechanism_alias` fires at ≥2, `agent_alias` at ≥5.
7. **Disambiguations**: `ticker_disambiguations.json` contains re-extraction rules for overly-generic fields (threshold, timeframe, mechanism, agent, target). Agent/target rules require higher frequency thresholds (5 and 4 respectively).
8. **Unresolvable FPs**: The disambiguation output reports `unresolvable_count` for false positives with no detectable field-level collapse — these remain pair-exclusion only.

### Full pipeline execution

```bash
python pipeline_daily_refresh.py --start-phase 5
```

Runs all of Phase 5 (web data generation) including the feedback steps in order. All four feedback steps are non-blocking (`required=False`).

---

## Feedback Payload Format

Submitted from the website to the Google Apps Script webhook:

```json
{
    "timestamp": "2026-03-10T08:00:00.000Z",
    "feedbackType": "same-event:same-rules",
    "notes": "Same market, different resolution mechanism",
    "markets": [
        {
            "key": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
            "label": "Will Trump win the 2028 presidential election?",
            "platform": "Both",
            "category": "Electoral"
        }
    ]
}
```

The `feedbackType` field encodes the label and sub-option:
- `same-event:same-rules` — same event, same resolution criteria
- `same-event:different-rules` — same event, different resolution criteria
- `different-event` — incorrectly matched markets
- `not-political`, `wrong-category`, `other` — standalone labels
