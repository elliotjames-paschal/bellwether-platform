# PredictionHunt Integration

## Overview

[PredictionHunt](https://www.predictionhunt.com) is a prediction market aggregator with a proprietary "Smart Matching" engine that maps equivalent markets across Kalshi, Polymarket, PredictIt, and ProphetX. We use their API as a **second opinion gate** before auto-applying cross-platform matches to our market map.

**Principle: PredictionHunt can veto, not create.** A match requires agreement from our pipeline + PredictionHunt. Disagreements trigger human review, not auto-rejection. PH having no data does NOT block a match (they may have less coverage than us).

## API Reference

- **Base URL:** `https://www.predictionhunt.com/api/v1`
- **Auth:** `X-API-Key` header
- **Env var:** `PREDICTIONHUNT_API_KEY`
- **Plan:** Dev tier, $50/month, 1,000 requests/month
- **Rate limits:** Per-second and per-month, returned in response headers
- **Docs:** https://www.predictionhunt.com/api/docs

### Endpoints

#### GET `/matching-markets`

Find equivalent markets across platforms.

| Parameter | Type | Description |
|-----------|------|-------------|
| `kalshi_tickers` | string | Kalshi **event** ticker (e.g., `KXPRESPERSON-28`, not market ID `KXPRESPERSON-28-JVAN`) |
| `polymarket_slugs` | string | Polymarket slug (e.g., `presidential-election-winner-2028`) |

Provide exactly one parameter per request.

#### GET `/matching-markets/url`

Same as above but accepts full URLs via `kalshi_url` or `polymarket_url` parameters.

### Response Schema

```json
{
  "success": true,
  "count": 1,
  "events": [
    {
      "title": "2028 Presidential Election",
      "event_date": "2028-11-05",
      "event_type": "election",
      "groups": [
        {
          "title": "J.D. Vance",
          "markets": [
            {
              "id": "561229",
              "source": "polymarket",
              "source_url": "https://polymarket.com/market/will-jd-vance-win-..."
            },
            {
              "id": "KXPRESPERSON-28-JVAN",
              "source": "kalshi",
              "source_url": "https://kalshi.com/markets/KXPRESPERSON/KXPRESPERSON-28"
            },
            {
              "id": "32018",
              "source": "predictit",
              "source_url": "https://www.predictit.org/markets/detail/8171/..."
            }
          ]
        }
      ]
    }
  ]
}
```

**Key details:**
- `events[].groups[]` groups markets by candidate/outcome
- Each group contains all platform markets PH considers equivalent
- `markets[].id` is the platform-native ID (Kalshi market ID, Polymarket short numeric ID, PredictIt contract ID)
- `markets[].source` is `"polymarket"`, `"kalshi"`, or `"predictit"`

### Rate Limit Headers

```
X-RateLimit-Limit-Second: 10
X-RateLimit-Remaining-Second: 9
X-RateLimit-Limit-Month: 1000
X-RateLimit-Remaining-Month: 842
```

429 responses include `Retry-After` header. Only successful responses (`success: true`) count toward monthly quota.

### Kalshi Ticker Format

PH expects **event-level** tickers, not market-level IDs:

| Our `k_ticker` (market ID) | PH expects (event ticker) | How we derive it |
|-----------------------------|---------------------------|------------------|
| `KXPRESPERSON-28-JVAN` | `KXPRESPERSON-28` | Strip last `-SEGMENT` |
| `KXPRESNOMD-28-TW` | `KXPRESNOMD-28` | Strip last `-SEGMENT` |
| `KXHONDURASPRESIDENTMOV-25NOV30-7` | `KXHONDURASPRESIDENTMOV-25NOV30` | Strip last `-SEGMENT` |

The function `kalshi_market_id_to_event_ticker()` in `predictionhunt_client.py` handles this conversion.

## Implementation

### Files

| File | Purpose |
|------|---------|
| `config.py` | `get_predictionhunt_api_key()` — reads `PREDICTIONHUNT_API_KEY` env var |
| `predictionhunt_client.py` | API client with rate limiting and budget tracking |
| `pipeline_validate_with_predictionhunt.py` | Standalone validation pipeline |
| `pipeline_update_matches.py` | Modified — calls PH gate before auto-applying IDENTICAL verdicts |

### Data Files

| File | Purpose |
|------|---------|
| `data/predictionhunt_usage.json` | Monthly budget tracking (auto-resets each month) |
| `data/predictionhunt_validation.json` | Full validation report from standalone pipeline |
| `data/predictionhunt_checked.json` | Checkpoint — which pairs have been checked (resumable) |
| `data/matches_pending_review.json` | Disagreements flagged for human review |

### How It Works

#### In the daily pipeline (`pipeline_update_matches.py`)

```
Embedding discovery → GPT verdict=IDENTICAL → PH gate → auto-apply or flag
```

1. After GPT classifies Bucket B candidates as IDENTICAL, the PH gate runs
2. For each IDENTICAL pair, queries PH with the Kalshi event ticker
3. Finds the specific group containing our Kalshi market ID
4. Checks if our Polymarket market ID is in the same group

**Outcomes:**
- `confirmed` — PH has the same Kalshi-Polymarket pairing → **auto-apply**
- `no_match` — PH doesn't have this market → **auto-apply** (don't block on PH coverage gaps)
- `disagreed` — PH pairs the Kalshi market with a different Polymarket market → **flag for review**
- `error` — API failure → **auto-apply** (don't block on PH outages)

Use `--skip-ph` flag to bypass the gate entirely.

### Client Usage

```python
from predictionhunt_client import PredictionHuntClient

client = PredictionHuntClient()

# Query by Kalshi market ID (auto-derives event ticker)
result = client.query_by_kalshi_ticker("KXPRESPERSON-28-JVAN", pipeline="my_pipeline")

# Query by Polymarket slug
result = client.query_by_polymarket_slug("presidential-election-winner-2028")

# Find PM matches for a specific Kalshi market within the response
pm_ids = client.find_group_for_kalshi_market(result, "KXPRESPERSON-28-JVAN")
# Returns: ["561229"]

# Check budget
remaining, used, limit = client.check_budget()
```

### CLI

```bash
# Test connectivity (uses 1 API request)
python predictionhunt_client.py --test

# Query specific tickers
python predictionhunt_client.py --kalshi KXPRESPERSON-28-JVAN
python predictionhunt_client.py --polymarket presidential-election-winner-2028

# Check usage
python predictionhunt_client.py --usage

# Run standalone validation
python pipeline_validate_with_predictionhunt.py --dry-run
python pipeline_validate_with_predictionhunt.py --limit 10
```

### Budget

- 1,000 requests/month on Dev tier ($50/month)
- Estimated usage: 50-200 requests/month (embedding IDENTICAL verdicts only)
- Budget tracked in `data/predictionhunt_usage.json`, auto-resets monthly
- Pipeline stops gracefully when budget exhausted, approves remaining without PH check
