# Combined Political Markets Data Glossary

**File**: `combined_political_markets.csv`
**Created**: 2025-11-19
**Total Markets**: 22,909 (14,333 Polymarket + 8,576 Kalshi)
**Total Columns**: 70

---

## Overview

This dataset combines political prediction markets from two platforms:
- **Polymarket**: Decentralized prediction market platform
- **Kalshi**: CFTC-regulated event contract exchange

The data structure includes:
1. **Standardized fields** (columns 1-15): Common fields mapped across both platforms
2. **Polymarket-specific fields** (columns 16-21): Fields with `pm_` prefix
3. **Kalshi-specific fields** (columns 22-70): Fields with `k_` prefix

---

## Standardized Fields (Both Platforms)

### Core Market Information

**platform**
- Type: String
- Values: "Polymarket" or "Kalshi"
- Description: The prediction market platform hosting this market

**market_id**
- Type: String
- Description: Unique identifier for the market
  - Polymarket: Numeric market ID (e.g., "240396")
  - Kalshi: Ticker symbol (e.g., "PRES-2024-TRUMP")

**question**
- Type: String
- Description: The market's question or title
- Example: "Will Donald Trump win the 2024 US Presidential Election?"

**political_category**
- Type: String
- Values: 15 categories (e.g., "1. ELECTORAL", "2. CABINET_APPOINTMENTS", etc.)
- Description: AI-classified political category using hierarchical taxonomy
- Note: Categories are numbered for sorting/grouping purposes

**election_type**
- Type: String (nullable)
- Values: "Presidential", "Senate", "House", "Gubernatorial", "Parliamentary", etc.
- Description: Type of election for electoral markets (null for non-electoral markets)

**party_affiliation**
- Type: String (nullable)
- Values: "Republican", "Democrat", or null
- Description: Party affiliation for candidate-specific markets

### Market Status

**is_closed**
- Type: Boolean
- Description: Whether the market is closed for trading
- Mapping:
  - Polymarket: `closed == True`
  - Kalshi: `status in ['closed', 'settled', 'finalized']`

### Resolution Information

**resolution_outcome**
- Type: String (nullable)
- Description: Raw resolution outcome from the platform
- Values:
  - Polymarket: Parsed from `outcomePrices` JSON (may be null)
  - Kalshi: "yes", "no", or null

**winning_outcome**
- Type: String (nullable)
- Values: "Yes", "No", or null
- Description: Standardized winning outcome
- Coverage: 99.4% of closed markets have this populated
- Note: 0.6% of closed markets missing due to edge cases in parsing

### Trading Metrics

**volume_usd**
- Type: Float
- Description: Total trading volume in US dollars
- Notes:
  - Polymarket: Direct USD volume from API
  - Kalshi: Number of contracts × $1 (notional_value = 100 cents)

### Timing Fields

**trading_close_time**
- Type: Datetime string (nullable)
- Description: When trading stopped/will stop
- Mapping:
  - Polymarket: `closedTime`
  - Kalshi: `close_time`
- Timezone: UTC

**scheduled_end_time**
- Type: Datetime string (nullable)
- Description: Expected market expiration/resolution time
- Mapping:
  - Polymarket: `endDate`
  - Kalshi: `expected_expiration_time`
- Timezone: UTC

---

## Election Data Fields (Polymarket Only)

These fields contain actual election outcome data merged from external sources.

**democrat_vote_share**
- Type: Float (nullable)
- Platform: Polymarket only (null for Kalshi)
- Description: Actual Democratic candidate vote share (0-1 scale)
- Example: 0.487 = 48.7%

**republican_vote_share**
- Type: Float (nullable)
- Platform: Polymarket only (null for Kalshi)
- Description: Actual Republican candidate vote share (0-1 scale)
- Example: 0.513 = 51.3%

**vote_share_source**
- Type: String (nullable)
- Platform: Polymarket only (null for Kalshi)
- Values: "AP", "CNN", "Ballotpedia", etc.
- Description: Source of the vote share data

---

## Polymarket-Specific Fields (pm_ prefix)

**pm_outcome_prices**
- Type: String (JSON array)
- Description: Final outcome prices from Polymarket
- Format: `["0.01", "0.99"]` or `["1", "0"]` for resolved markets
- Note: Index 0 = Yes outcome, Index 1 = No outcome
- Used to determine `winning_outcome`

**pm_token_id_yes**
- Type: String (nullable)
- Description: Polymarket's token ID for the Yes outcome
- Used for fetching price history

**pm_token_id_no**
- Type: String (nullable)
- Description: Polymarket's token ID for the No outcome
- Used for fetching price history

**pm_has_price_data**
- Type: Boolean (nullable)
- Description: Whether historical price data is available for this market

**pm_uma_resolution_status**
- Type: String (nullable)
- Description: UMA protocol resolution status
- Note: Does not directly indicate winner; use `pm_outcome_prices` instead

**pm_closed**
- Type: Boolean (nullable)
- Description: Original `closed` field from Polymarket API
- Note: Standardized version is in `is_closed`

---

## Kalshi-Specific Fields (k_ prefix)

### Market Identification

**k_event_ticker**
- Type: String (nullable)
- Description: Ticker for the parent event series
- Example: "PRES-2024" for individual "PRES-2024-TRUMP" market

**k_market_type**
- Type: String (nullable)
- Values: "binary", "multi", etc.
- Description: Type of market contract

**k_category**
- Type: String (nullable)
- Description: Kalshi's internal category classification
- Note: Different from our `political_category` field

**k_ai_classified_political**
- Type: Boolean (nullable)
- Description: Whether our AI classifier identified this as political

### Market Content

**k_subtitle**
- Type: String (nullable)
- Description: Additional context or subtitle for the market

**k_yes_sub_title**
- Type: String (nullable)
- Description: Description of what a Yes outcome represents

**k_no_sub_title**
- Type: String (nullable)
- Description: Description of what a No outcome represents

### Market Timing

**k_open_time**
- Type: Datetime string (nullable)
- Description: When the market opened for trading
- Timezone: UTC

**k_expiration_time**
- Type: Datetime string (nullable)
- Description: When the market contract expires
- Timezone: UTC

**k_latest_expiration_time**
- Type: Datetime string (nullable)
- Description: Latest possible expiration time (for extendable markets)
- Timezone: UTC

**k_settlement_timer_seconds**
- Type: Integer (nullable)
- Description: Seconds until market settles after expiration

### Market Status & Resolution

**k_status**
- Type: String (nullable)
- Values: "initialized", "active", "closed", "settled", "finalized"
- Description: Detailed market lifecycle status
- Note: Simplified to boolean in `is_closed`

**k_settlement_value**
- Type: Integer (nullable)
- Values: 0 or 100
- Description: Final settlement value in cents (0 = No, 100 = Yes)

**k_settlement_value_dollars**
- Type: Float (nullable)
- Description: Settlement value in dollars ($0.00 or $1.00)

**k_expiration_value**
- Type: Integer (nullable)
- Description: Value at expiration in cents

### Trading Metrics

**k_notional_value**
- Type: Integer (nullable)
- Value: Typically 100
- Description: Notional value of each contract in cents ($1.00)

**k_notional_value_dollars**
- Type: Float (nullable)
- Value: Typically 1.0
- Description: Notional value in dollars

**k_volume_contracts**
- Type: Integer (nullable)
- Description: Total number of contracts traded
- Note: Equivalent to `volume_usd` since contracts are $1 each

**k_volume_24h**
- Type: Integer (nullable)
- Description: Trading volume in last 24 hours (in contracts)

**k_liquidity**
- Type: Integer (nullable)
- Description: Market liquidity in cents

**k_liquidity_dollars**
- Type: Float (nullable)
- Description: Market liquidity in dollars

**k_open_interest**
- Type: Integer (nullable)
- Description: Number of outstanding contracts

**k_risk_limit_cents**
- Type: Integer (nullable)
- Description: Maximum risk exposure allowed per user in cents

### Current Prices

**k_yes_bid**
- Type: Integer (nullable)
- Range: 0-100
- Description: Current best bid price for Yes outcome (in cents)

**k_yes_ask**
- Type: Integer (nullable)
- Range: 0-100
- Description: Current best ask price for Yes outcome (in cents)

**k_no_bid**
- Type: Integer (nullable)
- Range: 0-100
- Description: Current best bid price for No outcome (in cents)

**k_no_ask**
- Type: Integer (nullable)
- Range: 0-100
- Description: Current best ask price for No outcome (in cents)

**k_last_price**
- Type: Integer (nullable)
- Range: 0-100
- Description: Most recent trade price (in cents)

**k_yes_bid_dollars**
- Type: Float (nullable)
- Range: 0.00-1.00
- Description: Yes bid price in dollars

**k_yes_ask_dollars**
- Type: Float (nullable)
- Range: 0.00-1.00
- Description: Yes ask price in dollars

**k_no_bid_dollars**
- Type: Float (nullable)
- Range: 0.00-1.00
- Description: No bid price in dollars

**k_no_ask_dollars**
- Type: Float (nullable)
- Range: 0.00-1.00
- Description: No ask price in dollars

**k_last_price_dollars**
- Type: Float (nullable)
- Range: 0.00-1.00
- Description: Last trade price in dollars

### Previous Prices

**k_previous_yes_bid**
- Type: Integer (nullable)
- Description: Previous best bid for Yes (in cents)

**k_previous_yes_ask**
- Type: Integer (nullable)
- Description: Previous best ask for Yes (in cents)

**k_previous_price**
- Type: Integer (nullable)
- Description: Previous trade price (in cents)

**k_previous_yes_bid_dollars**
- Type: Float (nullable)
- Description: Previous Yes bid in dollars

**k_previous_yes_ask_dollars**
- Type: Float (nullable)
- Description: Previous Yes ask in dollars

**k_previous_price_dollars**
- Type: Float (nullable)
- Description: Previous trade price in dollars

### Market Rules

**k_response_price_units**
- Type: String (nullable)
- Values: "cents", "dollars"
- Description: Units used for price responses from API

**k_rules_primary**
- Type: String (nullable)
- Description: Primary rules governing market resolution

**k_rules_secondary**
- Type: String (nullable)
- Description: Secondary or clarifying rules

**k_can_close_early**
- Type: Boolean (nullable)
- Description: Whether market can close before scheduled expiration

**k_early_close_condition**
- Type: String (nullable)
- Description: Conditions under which market closes early

### Strike Information

**k_tick_size**
- Type: Integer (nullable)
- Description: Minimum price increment for trades (in cents)

**k_strike_type**
- Type: String (nullable)
- Values: "binary", "range", etc.
- Description: Type of strike price structure

**k_custom_strike**
- Type: String (nullable)
- Description: Custom strike value for range markets

**k_floor_strike**
- Type: Float (nullable)
- Description: Minimum strike value for range markets

**k_cap_strike**
- Type: Float (nullable)
- Description: Maximum strike value for range markets

---

## Data Quality Notes

### Coverage Statistics

- **Total Markets**: 22,909
- **Polymarket**: 14,333 (62.6%)
- **Kalshi**: 8,576 (37.4%)
- **Electoral Markets**: 6,381 (27.9%)
- **Closed Markets**: 18,901 (82.5%)
- **Closed Markets with Outcomes**: 18,793 (99.4% of closed)

### Known Limitations

1. **Missing Outcomes (0.6% of closed markets)**
   - 108 closed markets missing `winning_outcome`
   - Cause: Edge cases where `outcomePrices` contains near-0/near-1 values instead of exact "0"/"1"
   - Example: `["0.0000001358804709", "0.9999998641195290"]`
   - Impact: Minimal - affects <1% of closed markets

2. **Platform-Specific Null Values**
   - Polymarket rows have null values for all `k_*` fields
   - Kalshi rows have null values for all `pm_*` fields and election data fields
   - This is expected and by design

3. **Timezone Consistency**
   - All timestamps are in UTC
   - No timezone conversions needed

4. **Volume Comparability**
   - Polymarket and Kalshi volumes are directly comparable in USD
   - Both represent actual USD traded (Kalshi contracts are $1 each)

---

## Usage Examples

### Filtering by Platform

```python
polymarket = df[df['platform'] == 'Polymarket']
kalshi = df[df['platform'] == 'Kalshi']
```

### Electoral Markets Only

```python
electoral = df[df['political_category'] == '1. ELECTORAL']
```

### Resolved Markets with Outcomes

```python
resolved = df[df['is_closed'] & df['winning_outcome'].notna()]
```

### Republican Candidate Markets

```python
gop = df[df['party_affiliation'] == 'Republican']
```

### High-Volume Markets

```python
high_volume = df[df['volume_usd'] > 1_000_000]  # Over $1M
```

---

## Related Files

- **Source Data**:
  - `market_categories_with_outcomes.csv` - Polymarket source
  - `kalshi_all_political_with_categories.json` - Kalshi source

- **Generation Script**:
  - `scripts/create_combined_markets.py` - Script that created this dataset

- **Analysis Scripts**: See `scripts/` folder for various analysis scripts that use this data

---

## Version History

**v1.0** (2025-11-19)
- Initial release
- Combined 14,333 Polymarket + 8,576 Kalshi markets
- 70 columns total
- 99.4% outcome coverage for closed markets

---

## Contact

For questions or issues with this dataset, please refer to the project documentation or contact the research team.
