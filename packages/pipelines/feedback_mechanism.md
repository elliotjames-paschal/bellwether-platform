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
│  generate_ticker_        │  Step 4: Extract error patterns → correction rules for NEXT run
│  corrections.py          │
└─────────────────────────┘
```

---

## Data Flow

### File Dependencies

| File | Created By | Consumed By |
|------|-----------|-------------|
| `human_labels.json` | pipeline_ingest_feedback.py | pipeline_apply_human_labels.py, pipeline_evaluate_matches.py, generate_ticker_corrections.py |
| `tickers_postprocessed.json` | postprocess_tickers.py | All four feedback scripts (read) |
| `match_accuracy_report.json` | pipeline_evaluate_matches.py | generate_ticker_corrections.py |
| `ticker_corrections.json` | generate_ticker_corrections.py | postprocess_tickers.py (NEXT run) |
| `near_matches.json` | pipeline_apply_human_labels.py | Website display |
| `cross_platform_reviewed_pairs.json` | pipeline_apply_human_labels.py | pipeline_discover_cross_platform.py (skip re-surfacing) |
| `cross_platform_candidates.json` | pipeline_discover_cross_platform.py | pipeline_evaluate_matches.py (suggested labels) |
| `cross_platform_resolution_verdicts.json` | pipeline_compare_resolutions.py | pipeline_evaluate_matches.py (suggested labels) |
| `combined_political_markets_with_electoral_details_UPDATED.csv` | Earlier pipeline phases | pipeline_apply_human_labels.py, pipeline_evaluate_matches.py |

All paths resolve to `data/` via `config.DATA_DIR`.

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
    "applied_action": null
}
```

**CLI:**
```bash
python pipeline_ingest_feedback.py --dry-run           # Preview without writing
python pipeline_ingest_feedback.py --csv-file /tmp/f.csv  # Read from local file
```

**Idempotency:** Running ingest twice produces 0 new labels on the second run (timestamp-based deduplication).

---

## Step 2: Apply Human Labels

**Script:** `pipeline_apply_human_labels.py`

**Behavior by label type:**

### `same_event_same_rules`
- Validates that the two tickers share the same agent, action, target, and timeframe
- If only mechanism/threshold differ: unifies to a single ticker in `tickers_postprocessed.json`
- Sets `match_source: "human"` and `human_label_id` on the unified entry
- If core identity fields differ: marks label `status: "needs_review"` (not auto-applied)

### `same_event_different_rules`
- Adds the pair to `near_matches.json` for website display
- These pairs are shown as "same event, different resolution" — not merged

### `different_event`
- Appends `_SPLIT` suffix to one ticker to break the incorrect match
- Adds pair to `cross_platform_reviewed_pairs.json` to prevent re-discovery

### `not_political`
- Sets category to `"16. NOT_POLITICAL"` in the master CSV

### `wrong_category`
- Attempts to parse the correct category from the description
- If a valid category name is found: recategorizes in the master CSV
- Otherwise: marks `status: "needs_review"`

**Status transitions:** `pending` → `applied` or `pending` → `needs_review`

**CLI:**
```bash
python pipeline_apply_human_labels.py --dry-run   # Preview changes
python pipeline_apply_human_labels.py              # Apply for real
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
python pipeline_evaluate_matches.py --verbose          # Print disagreements + suggested labels
python pipeline_evaluate_matches.py --max-suggestions 50  # More suggestions
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

## Step 4: Generate Ticker Corrections

**Script:** `generate_ticker_corrections.py`

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
    "source_labels": ["hl_a1b2c3", "hl_d4e5f6", ...]
}
```

**How corrections are consumed:** On the NEXT pipeline run, `postprocess_tickers.py` reads `ticker_corrections.json` and applies each alias rule to matching tickers before any other processing. This closes the feedback loop.

**CLI:**
```bash
python generate_ticker_corrections.py --dry-run                    # Preview
python generate_ticker_corrections.py --min-frequency 3            # Uniform threshold
python generate_ticker_corrections.py --min-freq-agent 3 --min-freq-mechanism 1  # Per-type overrides
```

---

## Website Integration

### Market Monitor Modal

Each market detail modal includes a feedback section at the bottom with the same labels and options as the sidebar "Help Us Match Markets" card:

- **Same Event (Cross-Platform Match)** — with sub-options:
  - Same Rules (resolution criteria identical)
  - Different Rules (same event, different resolution)
- **Different Events** — markets incorrectly matched
- **Not Political** — market isn't about politics
- **Wrong Category** — mislabeled political category
- **Other Issue** — freeform

Submissions go to the Google Sheet via Apps Script webhook and are also stored in `localStorage.marketFeedback` as backup.

### Sidebar Review Mode

The sidebar "Help Us Match Markets" card activates review mode:
1. Checkboxes appear on all market cards
2. User selects 2+ markets and clicks "Submit Feedback"
3. Feedback modal opens with the same label options
4. Submission includes all selected market keys with platform and category metadata

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

# Step 2: Apply — modifies tickers_postprocessed.json + master CSV
python pipeline_apply_human_labels.py --dry-run        # Preview
python pipeline_apply_human_labels.py                  # Apply

# Step 3: Evaluate — creates data/match_accuracy_report.json
python pipeline_evaluate_matches.py --verbose

# Step 4: Corrections — creates data/ticker_corrections.json
python generate_ticker_corrections.py --dry-run        # Preview
python generate_ticker_corrections.py                  # Write
```

### What to verify

1. **Ingestion**: Only rows after `last_ingested_timestamp` appear. Run twice — second run adds 0 labels.
2. **Apply**: `same_event_same_rules` labels where agent/action/target differ should show `status: "needs_review"`, not `"applied"`.
3. **Evaluate**: Precision/recall numbers are plausible. Suggested labels rank uncertain pairs above high-volume-but-obvious ones.
4. **Corrections**: Rules match expected alias patterns. `mechanism_alias` fires at ≥2, `agent_alias` at ≥5.

### Full pipeline execution

```bash
python pipeline_daily_refresh.py --start-phase 5
```

Runs all of Phase 5 (web data generation) including the feedback steps in order.

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
