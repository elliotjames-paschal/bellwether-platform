# bellwether-mcp

An MCP server for [Bellwether](https://bellwethermetrics.com) prediction market data. Bellwether is a Stanford GSB research project that aggregates political prediction markets from Polymarket and Kalshi into a single, standardized dataset with live VWAP pricing and editorial reportability scores. This MCP server gives AI agents direct access to live market prices, search, and data quality assessments for over 8,000 active political markets.

## Installation

```bash
npx bellwether-mcp
```

Or install globally:

```bash
npm install -g bellwether-mcp
```

## Claude Desktop Configuration

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "bellwether": {
      "command": "npx",
      "args": ["bellwether-mcp"]
    }
  }
}
```

### With API Key (Optional)

```json
{
  "mcpServers": {
    "bellwether": {
      "command": "npx",
      "args": ["bellwether-mcp"],
      "env": {
        "BELLWETHER_API_KEY": "your-key-here"
      }
    }
  }
}
```

## Tools

### search_markets

Search active political prediction markets by topic or keyword.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| query | string | yes | Topic or keyword (e.g., "senate", "federal reserve") |
| category | string | no | Filter by category (see categories below) |
| limit | number | no | Max results, default 10, max 50 |

**Example output:**
```json
{
  "results": [
    {
      "ticker": "BWR-FED-CUT-FFR-SPECIFIC_MEETING-ANY-JUN2026",
      "title": "Will the Federal Reserve Cut rates by 25bps at their March 2026 meeting?",
      "category": "MONETARY_POLICY",
      "volume_usd": 5321454.70,
      "is_matched": true,
      "platforms": ["polymarket", "kalshi"]
    }
  ],
  "total": 51,
  "query": "federal reserve",
  "category": "MONETARY_POLICY"
}
```

### get_market

Get live price, VWAP, and data quality tier for a specific market.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| ticker | string | yes | Bellwether ticker (e.g., BWR-DEM-CONTROL-HOUSE-CERTIFIED-ANY-2026) |

**Example output:**
```json
{
  "ticker": "BWR-DEM-CONTROL-HOUSE-CERTIFIED-ANY-2026",
  "title": "Will the Democratic party win the House?",
  "bellwether_price": 0.8167,
  "price_label": "6h VWAP across platforms",
  "price_tier": 1,
  "reportability": "caution",
  "guidance": "Cite with context. Liquidity is moderate — note the market's liquidity level alongside any price you report. Cost to move price 5 cents: $41776.",
  "cost_to_move_5c": 41776,
  "trade_count": 130,
  "fetched_at": "2026-03-04T22:35:05.736Z"
}
```

### get_top_markets

Get the most actively traded prediction markets, optionally filtered by category.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| category | string | no | Filter by category |
| limit | number | no | Max results, default 10, max 50 |

**Example output:**
```json
{
  "results": [
    {
      "ticker": "BWR-TRUMP-APPOINT-SHELTON-ANNOUNCED-ANY-2028",
      "title": "Will Trump next nominate Judy Shelton as Fed Chair?",
      "category": "APPOINTMENTS",
      "volume_usd": 98200157,
      "is_matched": false,
      "platforms": ["kalshi"]
    }
  ],
  "category": null,
  "total_active": 8107
}
```

### get_platform_spread

Compare prices between Kalshi and Polymarket for a cross-platform matched market.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| ticker | string | yes | Bellwether ticker for a matched market (is_matched: true) |

**Example output:**
```json
{
  "ticker": "BWR-DEM-CONTROL-HOUSE-CERTIFIED-ANY-2026",
  "kalshi_price": 0.82,
  "polymarket_price": 0.84,
  "spread_cents": 2,
  "spread_direction": "polymarket_higher",
  "reportability": "caution",
  "fetched_at": "2026-03-04T22:35:06.417Z"
}
```

### get_reportability

Check whether a market is safe to cite in published content.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| ticker | string | yes | Bellwether ticker or slug |

**Example output:**
```json
{
  "ticker": "BWR-DEM-CONTROL-HOUSE-CERTIFIED-ANY-2026",
  "reportability": "caution",
  "cost_to_move_5c": 41776,
  "guidance": "Cite with context. Liquidity is moderate — note the market's liquidity level alongside any price you report. Cost to move price 5 cents: $41776.",
  "price_tier": 1,
  "fetched_at": "2026-03-04T22:35:07.806Z"
}
```

## Example Agent Workflow

```
Step 1: search_markets("2026 midterms")
  → Find relevant markets and their tickers

Step 2: get_market("BWR-DEM-CONTROL-HOUSE-CERTIFIED-ANY-2026")
  → Get live VWAP price and trading data

Step 3: get_reportability("BWR-DEM-CONTROL-HOUSE-CERTIFIED-ANY-2026")
  → Check if the price is safe to cite in a report
```

## Categories

`ELECTORAL`, `MONETARY_POLICY`, `INTERNATIONAL`, `POLITICAL_SPEECH`, `MILITARY_SECURITY`, `APPOINTMENTS`, `TIMING_EVENTS`, `JUDICIAL`, `PARTY_POLITICS`, `GOVERNMENT_OPERATIONS`, `REGULATORY`, `LEGISLATIVE`, `POLLING_APPROVAL`, `STATE_LOCAL`, `CRISIS_EMERGENCY`

## Documentation

Full API documentation: https://bellwethermetrics.com/docs.html

## Contact

paschal@stanford.edu
