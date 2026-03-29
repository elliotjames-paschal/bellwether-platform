/**
 * Bellwether Live Data Server (Cloudflare Workers version)
 *
 * Tiered Pricing System:
 * - Tier 1: 6h VWAP (10+ trades) - Full reportability
 * - Tier 2: 12h/24h VWAP (10+ trades) - Reportability downgraded one level
 * - Tier 3: Stale VWAP or insufficient data - Always Fragile
 *
 * Deploy: npx wrangler deploy
 */

// =============================================================================
// CONFIGURATION
// =============================================================================

// Native APIs - no authentication required for public data
const KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2";
const POLYMARKET_CLOB_BASE = "https://clob.polymarket.com";
const UPSTREAM_TIMEOUT_MS = 8000;

async function fetchWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

const CONFIG = {
  cache_ttl_ms: 30000, // 30 seconds cache TTL
  min_trades_for_vwap: 10,
  vwap_windows: [6, 12, 24],
};

// =============================================================================
// NATIVE API FUNCTIONS (Kalshi Elections API + Polymarket CLOB)
// =============================================================================

async function fetchKalshiOrderbook(ticker) {
  const url = `${KALSHI_API_BASE}/markets/${ticker}/orderbook`;

  try {
    const response = await fetchWithTimeout(url, { headers: { "Accept": "application/json" } });
    if (!response.ok) return null;

    const data = await response.json();
    const bids = [];
    const asks = [];

    // Kalshi API returns orderbook_fp (dollars) or legacy orderbook (cents)
    const obFp = data.orderbook_fp;
    const obLegacy = data.orderbook || data;

    if (obFp && (obFp.yes_dollars || obFp.no_dollars)) {
      for (const [priceStr, qtyStr] of (obFp.yes_dollars || [])) {
        const price = Number(priceStr);
        const size = Number(qtyStr);
        if (price > 0 && size > 0) bids.push({ price, size });
      }
      for (const [priceStr, qtyStr] of (obFp.no_dollars || [])) {
        const price = 1 - Number(priceStr);
        const size = Number(qtyStr);
        if (price > 0 && price < 1 && size > 0) asks.push({ price, size });
      }
    } else {
      for (const [priceVal, qty] of (obLegacy.yes || [])) {
        const price = Number(priceVal) / 100;
        const size = Number(qty);
        if (price > 0 && size > 0) bids.push({ price, size });
      }
      for (const [priceVal, qty] of (obLegacy.no || [])) {
        const price = 1 - Number(priceVal) / 100;
        const size = Number(qty);
        if (price > 0 && price < 1 && size > 0) asks.push({ price, size });
      }
    }

    bids.sort((a, b) => b.price - a.price);
    asks.sort((a, b) => a.price - b.price);

    return [bids, asks];
  } catch (err) {
    console.error(`Kalshi orderbook fetch error: ${err}`);
    return null;
  }
}

async function fetchPolymarketOrderbook(tokenId) {
  const url = `${POLYMARKET_CLOB_BASE}/book?token_id=${tokenId}`;

  try {
    const response = await fetchWithTimeout(url, { headers: { "Accept": "application/json" } });
    if (!response.ok) return null;

    const data = await response.json();
    const bids = [];
    const asks = [];

    for (const bid of (data.bids || [])) {
      const price = Number(bid.price || bid.p);
      const size = Number(bid.size || bid.s);
      if (price > 0 && size > 0) bids.push({ price, size });
    }
    for (const ask of (data.asks || [])) {
      const price = Number(ask.price || ask.p);
      const size = Number(ask.size || ask.s);
      if (price > 0 && size > 0) asks.push({ price, size });
    }

    bids.sort((a, b) => b.price - a.price);
    asks.sort((a, b) => a.price - b.price);

    return [bids, asks];
  } catch (err) {
    console.error(`Polymarket orderbook fetch error: ${err}`);
    return null;
  }
}

async function fetchOrderbook(platform, tokenId) {
  if (platform === "kalshi") {
    return fetchKalshiOrderbook(tokenId);
  } else {
    return fetchPolymarketOrderbook(tokenId);
  }
}

async function fetchKalshiTrades(ticker, windowHours) {
  const nowSec = Math.floor(Date.now() / 1000);
  const startSec = nowSec - (windowHours * 60 * 60);

  const allTrades = [];
  let cursor = null;
  const limit = 1000;
  const maxPages = 10;

  for (let page = 0; page < maxPages; page++) {
    const params = new URLSearchParams({
      ticker: ticker,
      limit: limit.toString(),
      min_ts: startSec.toString(),
      max_ts: (nowSec + 1).toString(),
    });
    if (cursor) params.set("cursor", cursor);

    const url = `${KALSHI_API_BASE}/markets/trades?${params}`;

    try {
      const response = await fetchWithTimeout(url, { headers: { "Accept": "application/json" } });
      if (!response.ok) break;

      const data = await response.json();
      const tradeList = data.trades || [];

      for (const trade of tradeList) {
        // Kalshi API returns yes_price_dollars (decimal string) or legacy yes_price (cents integer)
        const price = trade.yes_price_dollars != null
          ? Number(trade.yes_price_dollars)
          : Number(trade.yes_price) / 100;
        const size = Number(trade.count_fp || trade.count || 1);
        let timestamp = trade.created_time;

        if (typeof timestamp === "string") {
          timestamp = new Date(timestamp).getTime();
        } else if (timestamp < 1e12) {
          timestamp = timestamp * 1000;
        }

        if (price > 0 && price <= 1) {
          allTrades.push({ price, size, timestamp });
        }
      }

      cursor = data.cursor;
      if (!cursor || tradeList.length < limit) break;

    } catch (err) {
      console.error(`Kalshi trades fetch error: ${err}`);
      break;
    }
  }

  return allTrades;
}

async function fetchPolymarketTrades(tokenId, windowHours) {
  const nowMs = Date.now();
  const startMs = nowMs - (windowHours * 60 * 60 * 1000);

  const allTrades = [];
  let cursor = null;
  const maxPages = 10;

  for (let page = 0; page < maxPages; page++) {
    const params = new URLSearchParams({ asset_id: tokenId });
    if (cursor) params.set("next_cursor", cursor);

    const url = `${POLYMARKET_CLOB_BASE}/trades?${params}`;

    try {
      const response = await fetchWithTimeout(url, { headers: { "Accept": "application/json" } });
      if (!response.ok) break;

      const data = await response.json();
      const tradeList = Array.isArray(data) ? data : (data.trades || data.data || []);

      for (const trade of tradeList) {
        const price = Number(trade.price || trade.p);
        const size = Number(trade.size || trade.s || trade.amount || 1);
        let timestamp = Number(trade.timestamp || trade.t || trade.time || trade.created_at);
        if (timestamp < 1e12) timestamp = timestamp * 1000;

        if (price > 0 && price <= 1 && timestamp >= startMs) {
          allTrades.push({ price, size, timestamp });
        }
      }

      cursor = data.next_cursor;
      if (!cursor || tradeList.length === 0) break;

    } catch (err) {
      console.error(`Polymarket trades fetch error: ${err}`);
      break;
    }
  }

  return allTrades;
}

async function fetchTrades(platform, tokenId, windowHours) {
  if (platform === "kalshi") {
    return fetchKalshiTrades(tokenId, windowHours);
  } else {
    return fetchPolymarketTrades(tokenId, windowHours);
  }
}

// =============================================================================
// CALCULATION FUNCTIONS
// =============================================================================

function computeVWAP(trades) {
  if (trades.length === 0) {
    return { vwap: null, trade_count: 0, total_volume: 0 };
  }

  let sumPriceVolume = 0;
  let sumVolume = 0;

  for (const trade of trades) {
    sumPriceVolume += trade.price * trade.size;
    sumVolume += trade.size;
  }

  return {
    vwap: sumVolume > 0 ? Math.round((sumPriceVolume / sumVolume) * 10000) / 10000 : null,
    trade_count: trades.length,
    total_volume: Math.round(sumVolume),
  };
}

// Cost to push price UP 5 cents (buying into asks)
function computeCostToMoveUp5Cents(asks) {
  if (asks.length === 0) return null;

  const startingPrice = asks[0].price;
  const targetPrice = startingPrice + 0.05;

  let spent = 0;

  for (const ask of asks) {
    if (ask.price >= targetPrice) {
      return Math.round(spent);
    }
    const levelCost = ask.price * ask.size;
    spent += levelCost;
  }

  return null; // Not enough depth
}

// Cost to push price DOWN 5 cents (selling into bids)
function computeCostToMoveDown5Cents(bids) {
  if (bids.length === 0) return null;

  const startingPrice = bids[0].price; // Best bid (highest)
  const targetPrice = startingPrice - 0.05;

  let value = 0; // Value of shares we need to sell

  for (const bid of bids) {
    if (bid.price <= targetPrice) {
      return Math.round(value);
    }
    const levelValue = bid.price * bid.size;
    value += levelValue;
  }

  return null; // Not enough depth
}

// Returns minimum cost to move price 5 cents in EITHER direction
// This is the vulnerability - manipulator picks the cheaper direction
function computeCostToMove5Cents(bids, asks) {
  const costUp = computeCostToMoveUp5Cents(asks);
  const costDown = computeCostToMoveDown5Cents(bids);

  if (costUp === null && costDown === null) return null;
  if (costUp === null) return costDown;
  if (costDown === null) return costUp;
  return Math.min(costUp, costDown);
}

function computeOrderbookMidpoint(bids, asks) {
  if (bids.length === 0 || asks.length === 0) return null;
  const bestBid = bids[0].price;
  const bestAsk = asks[0].price;
  return Math.round(((bestBid + bestAsk) / 2) * 10000) / 10000;
}

function getBaseReportability(costToMove5c) {
  if (costToMove5c === null || costToMove5c < 10000) return "fragile";
  if (costToMove5c < 100000) return "caution";
  return "reportable";
}

function downgradeReportability(r) {
  if (r === "reportable") return "caution";
  if (r === "caution") return "fragile";
  return "fragile";
}

function capReportability(r, maxLevel) {
  const levels = ["fragile", "caution", "reportable"];
  const currentIdx = levels.indexOf(r);
  const maxIdx = levels.indexOf(maxLevel);
  return levels[Math.min(currentIdx, maxIdx)];
}

// =============================================================================
// CACHE FUNCTIONS (using Cloudflare KV)
// =============================================================================

async function getCachedMetrics(kv, platform, tokenId) {
  if (!kv) return null;

  try {
    const cached = await kv.get(`${platform}:${tokenId}`, { type: "json" });
    if (!cached) return null;

    const fetchedAt = new Date(cached.fetched_at).getTime();
    if (Date.now() - fetchedAt > CONFIG.cache_ttl_ms) {
      return null;
    }

    return { ...cached, cached: true };
  } catch (err) {
    console.error("Cache read error:", err);
    return null;
  }
}

async function cacheMetrics(kv, platform, tokenId, metrics) {
  if (!kv) return;

  try {
    await kv.put(`${platform}:${tokenId}`, JSON.stringify(metrics), {
      expirationTtl: Math.ceil(CONFIG.cache_ttl_ms / 1000),
    });
  } catch (err) {
    console.error("Cache write error:", err);
  }
}

async function getStaleVWAP(kv, prefix, key) {
  if (!kv) return null;

  try {
    return await kv.get(`stale:${prefix}:${key}`, { type: "json" });
  } catch (err) {
    return null;
  }
}

async function storeStaleVWAP(kv, prefix, key, price, windowHours, tradeCount) {
  if (!kv) return;

  try {
    const stale = {
      price,
      window_hours: windowHours,
      trade_count: tradeCount,
      stored_at: new Date().toISOString(),
    };
    // Store for 7 days
    await kv.put(`stale:${prefix}:${key}`, JSON.stringify(stale), { expirationTtl: 604800 });
  } catch (err) {
    console.error("Stale VWAP store error:", err);
  }
}

// =============================================================================
// TIERED PRICE CALCULATION
// =============================================================================

async function computeTieredPrice(platform, tokenId, bids, asks, kv) {
  // Fetch 24h trades once, then filter for smaller windows in memory
  const allTrades = await fetchTrades(platform, tokenId, 24);
  const now = Date.now();

  // Try progressively larger windows by filtering the same trade data
  for (const windowHours of CONFIG.vwap_windows) {
    const cutoff = now - (windowHours * 60 * 60 * 1000);
    const windowTrades = allTrades.filter(t => t.timestamp >= cutoff);
    const vwapResult = computeVWAP(windowTrades);

    if (vwapResult.trade_count >= CONFIG.min_trades_for_vwap) {
      // Success! Store this as the last known good VWAP
      await storeStaleVWAP(kv, platform, tokenId, vwapResult.vwap, windowHours, vwapResult.trade_count);

      const tier = windowHours === 6 ? 1 : 2;
      const source = windowHours === 6 ? "6h_vwap" : (windowHours === 12 ? "12h_vwap" : "24h_vwap");
      const label = `${windowHours}h VWAP`;

      return {
        tier,
        price: vwapResult.vwap,
        label,
        window_hours: windowHours,
        trade_count: vwapResult.trade_count,
        total_volume: vwapResult.total_volume,
        source,
      };
    }
  }

  // No sufficient trades - try stale VWAP
  const stale = await getStaleVWAP(kv, platform, tokenId);
  if (stale) {
    return {
      tier: 3,
      price: stale.price,
      label: "Stale VWAP",
      window_hours: stale.window_hours,
      trade_count: stale.trade_count,
      total_volume: 0,
      source: "stale_vwap",
    };
  }

  // No data at all - return null price, always fragile
  return {
    tier: 3,
    price: null,
    label: "Insufficient data",
    window_hours: null,
    trade_count: 0,
    total_volume: 0,
    source: "no_data",
  };
}

async function computeCrossplatformTieredPrice(pmToken, kTicker, pmBids, pmAsks, kBids, kAsks, kv) {
  // Combine orderbooks for midpoint calculation
  const allBids = [...pmBids, ...kBids].sort((a, b) => b.price - a.price);
  const allAsks = [...pmAsks, ...kAsks].sort((a, b) => a.price - b.price);

  const cacheKey = `${pmToken || ""}_${kTicker || ""}`;

  // Fetch 24h trades once from each platform, then filter for smaller windows
  const allTrades = [];
  if (pmToken) {
    const pmTrades = await fetchTrades("polymarket", pmToken, 24);
    allTrades.push(...pmTrades);
  }
  if (kTicker) {
    const kTrades = await fetchTrades("kalshi", kTicker, 24);
    allTrades.push(...kTrades);
  }

  const now = Date.now();

  // Try progressively larger windows by filtering the same trade data
  for (const windowHours of CONFIG.vwap_windows) {
    const cutoff = now - (windowHours * 60 * 60 * 1000);
    const windowTrades = allTrades.filter(t => t.timestamp >= cutoff);
    const vwapResult = computeVWAP(windowTrades);

    if (vwapResult.trade_count >= CONFIG.min_trades_for_vwap) {
      await storeStaleVWAP(kv, "combined", cacheKey, vwapResult.vwap, windowHours, vwapResult.trade_count);

      const tier = windowHours === 6 ? 1 : 2;
      const source = windowHours === 6 ? "6h_vwap" : (windowHours === 12 ? "12h_vwap" : "24h_vwap");
      const label = `${windowHours}h VWAP across platforms`;

      return {
        tier,
        price: vwapResult.vwap,
        label,
        window_hours: windowHours,
        trade_count: vwapResult.trade_count,
        total_volume: vwapResult.total_volume,
        source,
      };
    }
  }

  // No sufficient trades - try stale VWAP
  const stale = await getStaleVWAP(kv, "combined", cacheKey);
  if (stale) {
    return {
      tier: 3,
      price: stale.price,
      label: "Stale VWAP",
      window_hours: stale.window_hours,
      trade_count: stale.trade_count,
      total_volume: 0,
      source: "stale_vwap",
    };
  }

  // No data at all - return null price, always fragile
  return {
    tier: 3,
    price: null,
    label: "Insufficient data",
    window_hours: null,
    trade_count: 0,
    total_volume: 0,
    source: "no_data",
  };
}

// =============================================================================
// MAIN FETCH FUNCTION
// =============================================================================

async function getMarketMetrics(platform, tokenId, kv) {
  const cached = await getCachedMetrics(kv, platform, tokenId);
  if (cached) {
    return cached;
  }

  const orderbook = await fetchOrderbook(platform, tokenId);
  if (!orderbook) {
    return null;
  }

  const [bids, asks] = orderbook;

  // Compute tiered price
  const tieredPrice = await computeTieredPrice(platform, tokenId, bids, asks, kv);

  // Compute robustness (min of up and down directions)
  const costToMove5c = computeCostToMove5Cents(bids, asks);
  const rawReportability = getBaseReportability(costToMove5c);

  // Adjust reportability based on tier
  let reportability;
  if (tieredPrice.tier === 1) {
    reportability = rawReportability;
  } else if (tieredPrice.tier === 2) {
    reportability = downgradeReportability(rawReportability);
  } else {
    // Tier 3 (stale/insufficient data) is always fragile
    reportability = "fragile";
  }

  // Get current price (most recent trade in any window)
  const recentTrades = await fetchTrades(platform, tokenId, 24);
  let currentPrice = null;
  if (recentTrades.length > 0) {
    const sortedTrades = [...recentTrades].sort((a, b) => b.timestamp - a.timestamp);
    currentPrice = sortedTrades[0].price;
  }

  const midpoint = computeOrderbookMidpoint(bids, asks);

  // Debug: compute both directions separately
  const costUp = computeCostToMoveUp5Cents(asks);
  const costDown = computeCostToMoveDown5Cents(bids);

  const metrics = {
    token_id: tokenId,
    platform,
    bellwether_price: tieredPrice.price,
    price_tier: tieredPrice.tier,
    price_label: tieredPrice.label,
    price_source: tieredPrice.source,
    current_price: currentPrice,
    robustness: {
      cost_to_move_5c: costToMove5c,
      cost_to_move_up_5c: costUp,
      cost_to_move_down_5c: costDown,
      reportability,
      raw_reportability: rawReportability,
    },
    vwap_details: {
      window_hours: tieredPrice.window_hours,
      trade_count: tieredPrice.trade_count,
      total_volume: tieredPrice.total_volume,
    },
    orderbook_midpoint: midpoint,
    orderbook_summary: {
      bid_levels: bids.length,
      ask_levels: asks.length,
      best_bid: bids.length > 0 ? bids[0].price : null,
      best_ask: asks.length > 0 ? asks[0].price : null,
      top_5_bids: bids.slice(0, 5).map(b => ({ price: b.price, size: b.size })),
      top_5_asks: asks.slice(0, 5).map(a => ({ price: a.price, size: a.size })),
    },
    fetched_at: new Date().toISOString(),
    cached: false,
  };

  await cacheMetrics(kv, platform, tokenId, metrics);

  return metrics;
}

// =============================================================================
// HTTP HANDLER (Cloudflare Workers format)
// =============================================================================

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const kv = env.BELLWETHER_KV || null;

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Content-Type": "application/json",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // GET /health
    if (url.pathname === "/health") {
      return new Response(
        JSON.stringify({
          status: "ok",
          mode: "cloudflare-workers",
          cache_ttl_seconds: CONFIG.cache_ttl_ms / 1000,
          kv_configured: !!kv,
          min_trades_for_vwap: CONFIG.min_trades_for_vwap,
          vwap_windows: CONFIG.vwap_windows,
        }),
        { headers: corsHeaders }
      );
    }

    // GET / - Basic info
    if (url.pathname === "/") {
      return new Response(
        JSON.stringify({
          name: "Bellwether Live Data Server",
          version: "3.0.0-cloudflare",
          description: "Tiered pricing: 6h VWAP → 12h/24h VWAP → Orderbook midpoint → Stale VWAP",
          endpoints: {
            "/health": "Server health check",
            "/api/metrics/:platform/:token_id": "Get tiered price + robustness for a single-platform market",
            "/api/metrics/combined": "Get cross-platform tiered price + min robustness (query: pm_token, k_ticker)",
          },
          price_tiers: {
            1: "6h VWAP (10+ trades) - Full reportability",
            2: "12h/24h VWAP (10+ trades) - Reportability downgraded one level",
            3: "Stale VWAP or insufficient data - Always Fragile",
          },
        }),
        { headers: corsHeaders }
      );
    }

    // GET /api/metrics/:platform/:token_id
    const metricsMatch = url.pathname.match(/^\/api\/metrics\/(polymarket|kalshi)\/(.+)$/);
    if (metricsMatch) {
      const platform = metricsMatch[1];
      const tokenId = metricsMatch[2];

      const metrics = await getMarketMetrics(platform, tokenId, kv);

      if (!metrics) {
        return new Response(
          JSON.stringify({
            error: "Failed to fetch market data",
            hint: "Check that the token_id is valid and the platform is correct"
          }),
          { status: 404, headers: corsHeaders }
        );
      }

      return new Response(JSON.stringify(metrics), { headers: corsHeaders });
    }

    // GET /api/metrics/combined - Cross-platform tiered price and min robustness
    if (url.pathname === "/api/metrics/combined") {
      const pmToken = url.searchParams.get("pm_token");
      const kTicker = url.searchParams.get("k_ticker");

      if (!pmToken && !kTicker) {
        return new Response(
          JSON.stringify({
            error: "Missing parameters",
            hint: "Provide at least one of: pm_token, k_ticker"
          }),
          { status: 400, headers: corsHeaders }
        );
      }

      // Fetch orderbooks from both platforms in parallel
      const [pmOrderbook, kOrderbook] = await Promise.all([
        pmToken ? fetchOrderbook("polymarket", pmToken) : null,
        kTicker ? fetchOrderbook("kalshi", kTicker) : null,
      ]);

      const pmBids = pmOrderbook?.[0] || [];
      const pmAsks = pmOrderbook?.[1] || [];
      const kBids = kOrderbook?.[0] || [];
      const kAsks = kOrderbook?.[1] || [];

      // Compute tiered price across platforms
      const tieredPrice = await computeCrossplatformTieredPrice(
        pmToken, kTicker, pmBids, pmAsks, kBids, kAsks, kv
      );

      // Use minimum robustness (weakest link across platforms AND directions)
      const pmCost = (pmBids.length > 0 || pmAsks.length > 0) ? computeCostToMove5Cents(pmBids, pmAsks) : null;
      const kCost = (kBids.length > 0 || kAsks.length > 0) ? computeCostToMove5Cents(kBids, kAsks) : null;

      let minCost = null;
      let weakestPlatform = "unknown";

      if (pmCost !== null && kCost !== null) {
        minCost = Math.min(pmCost, kCost);
        weakestPlatform = pmCost <= kCost ? "polymarket" : "kalshi";
      } else if (pmCost !== null) {
        minCost = pmCost;
        weakestPlatform = "polymarket";
      } else if (kCost !== null) {
        minCost = kCost;
        weakestPlatform = "kalshi";
      }

      const rawReportability = getBaseReportability(minCost);

      // Adjust reportability based on tier
      let reportability;
      if (tieredPrice.tier === 1) {
        reportability = rawReportability;
      } else if (tieredPrice.tier === 2) {
        reportability = downgradeReportability(rawReportability);
      } else {
        // Tier 3 (stale/insufficient data) is always fragile
        reportability = "fragile";
      }

      // Get current prices from each platform
      let pmCurrentPrice = null;
      let kCurrentPrice = null;

      if (pmToken) {
        const pmTrades = await fetchTrades("polymarket", pmToken, 24);
        if (pmTrades.length > 0) {
          pmCurrentPrice = [...pmTrades].sort((a, b) => b.timestamp - a.timestamp)[0].price;
        }
      }
      if (kTicker) {
        const kTrades = await fetchTrades("kalshi", kTicker, 24);
        if (kTrades.length > 0) {
          kCurrentPrice = [...kTrades].sort((a, b) => b.timestamp - a.timestamp)[0].price;
        }
      }

      const combined = {
        bellwether_price: tieredPrice.price,
        price_tier: tieredPrice.tier,
        price_label: tieredPrice.label,
        price_source: tieredPrice.source,
        platform_prices: {
          polymarket: pmCurrentPrice,
          kalshi: kCurrentPrice,
        },
        robustness: {
          cost_to_move_5c: minCost,
          reportability,
          raw_reportability: rawReportability,
          weakest_platform: weakestPlatform,
        },
        vwap_details: {
          window_hours: tieredPrice.window_hours,
          trade_count: tieredPrice.trade_count,
          total_volume: tieredPrice.total_volume,
        },
        orderbook_midpoint: computeOrderbookMidpoint(
          [...pmBids, ...kBids].sort((a, b) => b.price - a.price),
          [...pmAsks, ...kAsks].sort((a, b) => a.price - b.price)
        ),
        fetched_at: new Date().toISOString(),
      };

      return new Response(JSON.stringify(combined), { headers: corsHeaders });
    }

    // Legacy endpoint support
    const legacyMatch = url.pathname.match(/^\/metrics\/(.+)$/);
    if (legacyMatch) {
      const tokenId = legacyMatch[1];
      const metrics = await getMarketMetrics("polymarket", tokenId, kv);

      if (!metrics) {
        return new Response(
          JSON.stringify({ error: "Market not found" }),
          { status: 404, headers: corsHeaders }
        );
      }

      return new Response(JSON.stringify(metrics), { headers: corsHeaders });
    }

    return new Response(
      JSON.stringify({
        error: "Not found",
        available_endpoints: ["/", "/health", "/api/metrics/:platform/:token_id", "/api/metrics/combined"]
      }),
      { status: 404, headers: corsHeaders }
    );
  },
};
