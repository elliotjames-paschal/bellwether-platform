# Bellwether Live Data Server

Lightweight caching layer for real-time prediction market data.

## What it does

1. **Manipulation Cost**: Simulates "$100K buy" on each market, reports price impact
2. **6-Hour VWAP**: Computes volume-weighted average price (Duffie method)
3. **Caches results**: One API connection serves all website visitors

## Quick Start

```bash
# Set your Dome API key
export DOME_API_KEY=your_key_here

# Run with Node.js
node live-data-server.js

# Server runs on http://localhost:3000
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | All computed metrics for all markets |
| `GET /metrics/:token_id` | Metrics for specific market |
| `GET /health` | Health check |

## Deployment Options

### Stanford Farmshare
```bash
# SSH to farmshare
ssh yoursunetid@rice.stanford.edu

# Clone repo and run
cd ~/bellwether-server
export DOME_API_KEY=xxx
nohup node live-data-server.js > server.log 2>&1 &

# Note: Process may be killed after inactivity
# Consider using screen or tmux
```

### Cloudflare Workers (Free)
```bash
npm install -g wrangler
wrangler login
wrangler publish
```

### Deno Deploy (Free)
```bash
# Deploy via GitHub integration at dash.deno.com
```

### Fly.io (Free tier: 3 VMs)
```bash
fly launch
fly deploy
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DOME_API_KEY` | Your Dome API key | Yes |
| `PORT` | HTTP port (default: 3000) | No |

## Response Format

```json
{
  "generated_at": "2026-02-11T12:00:00Z",
  "manipulation_test_amount": 100000,
  "vwap_window_hours": 6,
  "markets": {
    "token_id_here": {
      "manipulation_cost": {
        "price_impact_cents": 2.3,
        "volume_consumed": 45000,
        "levels_consumed": 12,
        "dollars_spent": 100000
      },
      "vwap_6h": 0.573,
      "updated_at": 1707649200000
    }
  }
}
```
