/**
 * Bellwether Live Data Server V2 (Cloudflare Workers)
 *
 * V2 changes from cloudflare-worker.js:
 * - Loads market index from KV (market_map:latest) instead of being stateless
 * - New endpoint: GET /api/metrics/event/:slug - resolve slug to live data
 * - All responses include `ticker` field when available
 * - Removed legacy /metrics/:tokenId endpoint
 *
 * Tiered Pricing System:
 * - Tier 1: 6h VWAP (10+ trades) - Full reportability
 * - Tier 2: 12h/24h VWAP (10+ trades) - Reportability downgraded one level
 * - Tier 3: Stale VWAP or insufficient data - Always Fragile
 *
 * Deploy: cd packages/website/server && npx wrangler deploy -c wrangler-v2.toml
 */

// =============================================================================
// CONFIGURATION
// =============================================================================

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
// MARKET MAP - Loaded from KV
// =============================================================================

let cachedMarketMap = null;
let marketMapTimestamp = 0;
const MARKET_MAP_CACHE_TTL = 300000; // 5 minutes

async function loadMarketMap(kv) {
  const now = Date.now();

  if (cachedMarketMap && (now - marketMapTimestamp) < MARKET_MAP_CACHE_TTL) {
    return cachedMarketMap;
  }

  if (!kv) return [];

  try {
    const data = await kv.get("market_map:latest", { type: "json" });
    if (data && data.markets) {
      cachedMarketMap = data.markets;
      marketMapTimestamp = now;
      return cachedMarketMap;
    }
  } catch (err) {
    console.error("Failed to load market_map from KV:", err);
  }

  // Return stale cache if KV read failed
  if (cachedMarketMap) return cachedMarketMap;

  return [];
}

function getMarketBySlug(markets, slug) {
  return markets.find(m =>
    m.slug === slug ||
    (m.ticker && m.ticker.toLowerCase().replace(/_/g, "-") === slug)
  );
}

// =============================================================================
// ACTIVE MARKETS - Loaded from KV (for /api/markets/* endpoints)
// =============================================================================

const VALID_CATEGORIES = [
  "ELECTORAL", "MONETARY_POLICY", "INTERNATIONAL", "POLITICAL_SPEECH",
  "MILITARY_SECURITY", "APPOINTMENTS", "TIMING_EVENTS", "JUDICIAL",
  "PARTY_POLITICS", "GOVERNMENT_OPERATIONS", "REGULATORY", "LEGISLATIVE",
  "POLLING_APPROVAL", "STATE_LOCAL", "CRISIS_EMERGENCY",
];

let cachedActiveMarkets = null;
let activeMarketsTimestamp = 0;
const ACTIVE_MARKETS_CACHE_TTL = 300000; // 5 minutes

async function loadActiveMarkets(kv) {
  const now = Date.now();

  if (cachedActiveMarkets && (now - activeMarketsTimestamp) < ACTIVE_MARKETS_CACHE_TTL) {
    return cachedActiveMarkets;
  }

  if (!kv) throw new Error("KV not configured");

  const data = await kv.get("active_markets:latest", { type: "json" });
  if (data && data.markets) {
    cachedActiveMarkets = data.markets;
    activeMarketsTimestamp = now;
    return cachedActiveMarkets;
  }

  // Return stale cache if KV read returned empty
  if (cachedActiveMarkets) return cachedActiveMarkets;

  return null;
}

/**
 * Extract bare category name from prefixed format.
 * e.g. "6. INTERNATIONAL" → "INTERNATIONAL"
 */
function extractCategory(rawCategory) {
  if (!rawCategory) return null;
  const dotIndex = rawCategory.indexOf(". ");
  if (dotIndex >= 0) return rawCategory.slice(dotIndex + 2);
  return rawCategory;
}

/**
 * Format a raw market record into the API response shape.
 */
function formatMarketResult(m) {
  const ticker = m.ticker || m.key || "";
  const slug = ticker.toLowerCase().replace(/_/g, "-");
  const isMatched = !!m.has_both;
  const platformRaw = m.platform || "";

  let platforms;
  if (isMatched) {
    platforms = ["polymarket", "kalshi"];
  } else if (platformRaw.toLowerCase() === "polymarket") {
    platforms = ["polymarket"];
  } else {
    platforms = ["kalshi"];
  }

  return {
    slug,
    ticker,
    title: m.label || "",
    category: extractCategory(m.category) || "",
    volume_usd: m.total_volume || 0,
    is_matched: isMatched,
    platforms,
  };
}

/**
 * Optional API key check for /api/markets/* routes.
 * Returns null if auth passes, or an error object { status, body } if it fails.
 */
function checkOptionalApiKey(request, env) {
  const expectedKey = env.BELLWETHER_API_KEY;
  if (!expectedKey) return null; // env var not set — skip auth

  const authHeader = request.headers.get("Authorization");
  if (!authHeader) return null; // no header — proceed unauthenticated

  const parts = authHeader.split(" ");
  if (parts.length === 2 && parts[0] === "Bearer" && parts[1] === expectedKey) {
    return null; // valid key
  }

  return {
    status: 401,
    body: { error: "invalid_api_key" },
  };
}

// =============================================================================
// NATIVE API FUNCTIONS (Kalshi Elections API + Polymarket CLOB)
// =============================================================================

async function fetchKalshiOrderbook(ticker) {
  const url = `${KALSHI_API_BASE}/markets/${ticker}/orderbook`;

  try {
    const response = await fetchWithTimeout(url, { headers: { "Accept": "application/json" } });
    if (!response.ok) return null;

    const data = await response.json();
    const orderbook = data.orderbook || data;
    const bids = [];
    const asks = [];

    const yesOrders = orderbook.yes || [];
    const noOrders = orderbook.no || [];

    for (const [priceVal, qty] of yesOrders) {
      const price = Number(priceVal) / 100;
      const size = Number(qty);
      if (price > 0 && size > 0) bids.push({ price, size });
    }

    for (const [priceVal, qty] of noOrders) {
      const noPrice = Number(priceVal) / 100;
      const price = 1 - noPrice;
      const size = Number(qty);
      if (price > 0 && price < 1 && size > 0) asks.push({ price, size });
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

async function resolvePolymarketConditionId(tokenId) {
  try {
    const url = `https://gamma-api.polymarket.com/markets?clob_token_ids=${tokenId}`;
    const response = await fetchWithTimeout(url, { headers: { "Accept": "application/json" } });
    if (!response.ok) return null;
    const data = await response.json();
    if (Array.isArray(data) && data.length > 0) {
      return data[0].conditionId || null;
    }
    return null;
  } catch (err) {
    console.error(`Polymarket conditionId lookup error: ${err}`);
    return null;
  }
}

async function fetchPolymarketTrades(tokenId, windowHours) {
  const nowMs = Date.now();
  const startMs = nowMs - (windowHours * 60 * 60 * 1000);

  const conditionId = await resolvePolymarketConditionId(tokenId);
  if (!conditionId) return [];

  const allTrades = [];
  let offset = 0;
  const pageSize = 1000;
  const maxPages = 10;

  for (let page = 0; page < maxPages; page++) {
    const params = new URLSearchParams({
      market: conditionId,
      limit: pageSize.toString(),
      offset: offset.toString(),
    });

    const url = `https://data-api.polymarket.com/trades?${params}`;

    try {
      const response = await fetchWithTimeout(url, { headers: { "Accept": "application/json" } });
      if (!response.ok) break;

      const tradeList = await response.json();
      if (!Array.isArray(tradeList) || tradeList.length === 0) break;

      for (const trade of tradeList) {
        // Only count trades for this specific token (YES side)
        if (trade.asset !== tokenId) continue;

        const price = Number(trade.price);
        const size = Number(trade.size || 1);
        let timestamp = Number(trade.timestamp);
        if (timestamp < 1e12) timestamp = timestamp * 1000;

        if (price > 0 && price <= 1 && timestamp >= startMs) {
          allTrades.push({ price, size, timestamp });
        }
      }

      if (tradeList.length < pageSize) break;
      offset += pageSize;

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

function computeCostToMoveUp5Cents(asks) {
  if (asks.length === 0) return null;
  const startingPrice = asks[0].price;
  const targetPrice = startingPrice + 0.05;
  let spent = 0;

  for (const ask of asks) {
    if (ask.price >= targetPrice) return Math.round(spent);
    spent += ask.price * ask.size;
  }

  return null;
}

function computeCostToMoveDown5Cents(bids) {
  if (bids.length === 0) return null;
  const startingPrice = bids[0].price;
  const targetPrice = startingPrice - 0.05;
  let value = 0;

  for (const bid of bids) {
    if (bid.price <= targetPrice) return Math.round(value);
    value += bid.price * bid.size;
  }

  return null;
}

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
  return Math.round(((bids[0].price + asks[0].price) / 2) * 10000) / 10000;
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
      expirationTtl: Math.ceil(CONFIG.cache_ttl_ms / 1000) + 60,
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
    await kv.put(`stale:${prefix}:${key}`, JSON.stringify(stale), { expirationTtl: 604800 });
  } catch (err) {
    console.error("Stale VWAP store error:", err);
  }
}

// =============================================================================
// TIERED PRICE CALCULATION
// =============================================================================

async function computeTieredPrice(platform, tokenId, bids, asks, kv) {
  const allTrades = await fetchTrades(platform, tokenId, 24);
  const now = Date.now();

  for (const windowHours of CONFIG.vwap_windows) {
    const cutoff = now - (windowHours * 60 * 60 * 1000);
    const windowTrades = allTrades.filter(t => t.timestamp >= cutoff);
    const vwapResult = computeVWAP(windowTrades);

    if (vwapResult.trade_count >= CONFIG.min_trades_for_vwap) {
      await storeStaleVWAP(kv, platform, tokenId, vwapResult.vwap, windowHours, vwapResult.trade_count);

      const tier = windowHours === 6 ? 1 : 2;
      const source = windowHours === 6 ? "6h_vwap" : (windowHours === 12 ? "12h_vwap" : "24h_vwap");

      return {
        tier,
        price: vwapResult.vwap,
        label: `${windowHours}h VWAP`,
        window_hours: windowHours,
        trade_count: vwapResult.trade_count,
        total_volume: vwapResult.total_volume,
        source,
      };
    }
  }

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
  const cacheKey = `${pmToken || ""}_${kTicker || ""}`;

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

  for (const windowHours of CONFIG.vwap_windows) {
    const cutoff = now - (windowHours * 60 * 60 * 1000);
    const windowTrades = allTrades.filter(t => t.timestamp >= cutoff);
    const vwapResult = computeVWAP(windowTrades);

    if (vwapResult.trade_count >= CONFIG.min_trades_for_vwap) {
      await storeStaleVWAP(kv, "combined", cacheKey, vwapResult.vwap, windowHours, vwapResult.trade_count);

      const tier = windowHours === 6 ? 1 : 2;
      const source = windowHours === 6 ? "6h_vwap" : (windowHours === 12 ? "12h_vwap" : "24h_vwap");

      return {
        tier,
        price: vwapResult.vwap,
        label: `${windowHours}h VWAP across platforms`,
        window_hours: windowHours,
        trade_count: vwapResult.trade_count,
        total_volume: vwapResult.total_volume,
        source,
      };
    }
  }

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
// MAIN FETCH FUNCTIONS
// =============================================================================

async function getMarketMetrics(platform, tokenId, kv) {
  const cached = await getCachedMetrics(kv, platform, tokenId);
  if (cached) return cached;

  const orderbook = await fetchOrderbook(platform, tokenId);
  if (!orderbook) return null;

  const [bids, asks] = orderbook;

  const tieredPrice = await computeTieredPrice(platform, tokenId, bids, asks, kv);

  const costToMove5c = computeCostToMove5Cents(bids, asks);
  const rawReportability = getBaseReportability(costToMove5c);

  let reportability;
  if (tieredPrice.tier === 1) {
    reportability = rawReportability;
  } else if (tieredPrice.tier === 2) {
    reportability = downgradeReportability(rawReportability);
  } else {
    reportability = "fragile";
  }

  const costUp = computeCostToMoveUp5Cents(asks);
  const costDown = computeCostToMoveDown5Cents(bids);
  const midpoint = computeOrderbookMidpoint(bids, asks);

  const metrics = {
    token_id: tokenId,
    platform,
    bellwether_price: tieredPrice.price,
    price_tier: tieredPrice.tier,
    price_label: tieredPrice.label,
    price_source: tieredPrice.source,
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

async function getEventMetrics(market, kv) {
  /**
   * Fetch live metrics for an event resolved via the market index.
   * Handles single-platform and cross-platform markets.
   */
  const pmToken = market.pm_token_id || market.pm_token;
  const kTicker = market.k_ticker;

  // Single-platform case
  if (!pmToken && kTicker) {
    const metrics = await getMarketMetrics("kalshi", kTicker, kv);
    if (metrics) metrics.ticker = market.ticker;
    return metrics;
  }
  if (pmToken && !kTicker) {
    const metrics = await getMarketMetrics("polymarket", pmToken, kv);
    if (metrics) metrics.ticker = market.ticker;
    return metrics;
  }

  // Cross-platform: combined metrics
  const [pmOrderbook, kOrderbook] = await Promise.all([
    pmToken ? fetchOrderbook("polymarket", pmToken) : null,
    kTicker ? fetchOrderbook("kalshi", kTicker) : null,
  ]);

  const pmBids = pmOrderbook?.[0] || [];
  const pmAsks = pmOrderbook?.[1] || [];
  const kBids = kOrderbook?.[0] || [];
  const kAsks = kOrderbook?.[1] || [];

  const tieredPrice = await computeCrossplatformTieredPrice(
    pmToken, kTicker, pmBids, pmAsks, kBids, kAsks, kv
  );

  const pmCost = (pmBids.length > 0 || pmAsks.length > 0) ? computeCostToMove5Cents(pmBids, pmAsks) : null;
  const kCost = (kBids.length > 0 || kAsks.length > 0) ? computeCostToMove5Cents(kBids, kAsks) : null;

  let minCost = null;
  let weakestPlatform = null;

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
  let reportability;
  if (tieredPrice.tier === 1) {
    reportability = rawReportability;
  } else if (tieredPrice.tier === 2) {
    reportability = downgradeReportability(rawReportability);
  } else {
    reportability = "fragile";
  }

  // Build merged orderbooks with platform tags
  const mergedBids = [
    ...pmBids.map(b => ({ ...b, platform: "polymarket" })),
    ...kBids.map(b => ({ ...b, platform: "kalshi" })),
  ].sort((a, b) => b.price - a.price);

  const mergedAsks = [
    ...pmAsks.map(a => ({ ...a, platform: "polymarket" })),
    ...kAsks.map(a => ({ ...a, platform: "kalshi" })),
  ].sort((a, b) => a.price - b.price);

  const costUp = computeCostToMoveUp5Cents(mergedAsks);
  const costDown = computeCostToMoveDown5Cents(mergedBids);

  return {
    ticker: market.ticker,
    bellwether_price: tieredPrice.price,
    price_tier: tieredPrice.tier,
    price_label: tieredPrice.label,
    price_source: tieredPrice.source,
    platform_prices: {
      polymarket: pmToken || null,
      kalshi: kTicker || null,
    },
    robustness: {
      cost_to_move_5c: minCost,
      cost_to_move_up_5c: costUp,
      cost_to_move_down_5c: costDown,
      reportability,
      raw_reportability: rawReportability,
      weakest_platform: weakestPlatform,
    },
    vwap_details: {
      window_hours: tieredPrice.window_hours,
      trade_count: tieredPrice.trade_count,
      total_volume: tieredPrice.total_volume,
    },
    orderbook_midpoint: computeOrderbookMidpoint(mergedBids, mergedAsks),
    orderbook_summary: {
      bid_levels: mergedBids.length,
      ask_levels: mergedAsks.length,
      best_bid: mergedBids.length > 0 ? mergedBids[0].price : null,
      best_ask: mergedAsks.length > 0 ? mergedAsks[0].price : null,
      top_5_bids: mergedBids.slice(0, 5).map(b => ({ price: b.price, size: b.size, platform: b.platform })),
      top_5_asks: mergedAsks.slice(0, 5).map(a => ({ price: a.price, size: a.size, platform: a.platform })),
    },
    fetched_at: new Date().toISOString(),
  };
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
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Content-Type": "application/json",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // GET /health
    if (url.pathname === "/health") {
      const markets = await loadMarketMap(kv);
      return new Response(
        JSON.stringify({
          status: "ok",
          version: "2.0.0",
          cache_ttl_seconds: CONFIG.cache_ttl_ms / 1000,
          kv_configured: !!kv,
          market_count: markets.length,
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
          version: "2.0.0",
          description: "V2: All active markets via KV market index",
          endpoints: {
            "/health": "Server health check",
            "/api/markets/:ticker": "Get live data for a market by BWR ticker (e.g. BWR-DEM-WIN-SENATE_TX-CERTIFIED-ANY-2026)",
            "/api/markets/search": "Search markets by keyword (query: q, category, limit)",
            "/api/markets/top": "Top markets by volume (query: category, limit)",
            "/api/metrics/:platform/:token_id": "Get tiered price + robustness for a single-platform market",
            "/api/metrics/combined": "Get cross-platform tiered price + min robustness (query: pm_token, k_ticker)",
            "/api/metrics/event/:slug": "Get live data for an event by Bellwether slug",
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

    // GET /api/metrics/event/:slug - Resolve slug via market index
    const slugMatch = url.pathname.match(/^\/api\/metrics\/event\/([a-z0-9-]+)$/);
    if (slugMatch) {
      const slug = slugMatch[1];
      const markets = await loadMarketMap(kv);
      const market = getMarketBySlug(markets, slug);

      if (!market) {
        return new Response(
          JSON.stringify({
            error: "Event not found",
            slug,
            hint: "Check /health for market_count to verify market index is loaded",
          }),
          { status: 404, headers: corsHeaders }
        );
      }

      const metrics = await getEventMetrics(market, kv);
      if (!metrics) {
        return new Response(
          JSON.stringify({ error: "Market data unavailable", slug }),
          { status: 502, headers: corsHeaders }
        );
      }

      return new Response(
        JSON.stringify({
          slug: market.slug,
          title: market.title,
          category: market.category,
          country: market.country,
          platform: market.platform,
          ...metrics,
        }),
        { headers: corsHeaders }
      );
    }

    // GET /api/markets/:ticker - Look up market by BWR ticker
    const tickerMatch = url.pathname.match(/^\/api\/markets\/([A-Z0-9_-]+)$/);
    if (tickerMatch) {
      const tickerParam = tickerMatch[1];
      const markets = await loadMarketMap(kv);
      const market = markets.find(m => m.ticker === tickerParam);

      if (!market) {
        return new Response(
          JSON.stringify({
            error: "Market not found",
            ticker: tickerParam,
            hint: "Use /api/markets/search?q=... to find available tickers",
          }),
          { status: 404, headers: corsHeaders }
        );
      }

      const metrics = await getEventMetrics(market, kv);
      if (!metrics) {
        return new Response(
          JSON.stringify({ error: "Market data unavailable", ticker: tickerParam }),
          { status: 502, headers: corsHeaders }
        );
      }

      return new Response(
        JSON.stringify({
          ticker: market.ticker,
          slug: market.slug,
          title: market.title,
          category: market.category,
          country: market.country,
          platforms: [
            market.pm_token_id || market.pm_token ? "polymarket" : null,
            market.k_ticker ? "kalshi" : null,
          ].filter(Boolean),
          volume_usd: market.total_volume || 0,
          ...metrics,
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

      // Try to find ticker from market index
      const markets = await loadMarketMap(kv);
      const match = markets.find(m =>
        (platform === "kalshi" && m.k_ticker === tokenId) ||
        (platform === "polymarket" && (m.pm_token_id === tokenId || m.pm_token === tokenId))
      );
      if (match) {
        metrics.ticker = match.ticker;
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

      const [pmOrderbook, kOrderbook] = await Promise.all([
        pmToken ? fetchOrderbook("polymarket", pmToken) : null,
        kTicker ? fetchOrderbook("kalshi", kTicker) : null,
      ]);

      const pmBids = pmOrderbook?.[0] || [];
      const pmAsks = pmOrderbook?.[1] || [];
      const kBids = kOrderbook?.[0] || [];
      const kAsks = kOrderbook?.[1] || [];

      const tieredPrice = await computeCrossplatformTieredPrice(
        pmToken, kTicker, pmBids, pmAsks, kBids, kAsks, kv
      );

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
      let reportability;
      if (tieredPrice.tier === 1) {
        reportability = rawReportability;
      } else if (tieredPrice.tier === 2) {
        reportability = downgradeReportability(rawReportability);
      } else {
        reportability = "fragile";
      }

      // Try to find ticker from market index
      let ticker = null;
      const markets = await loadMarketMap(kv);
      const match = markets.find(m =>
        (kTicker && m.k_ticker === kTicker) ||
        (pmToken && (m.pm_token_id === pmToken || m.pm_token === pmToken))
      );
      if (match) ticker = match.ticker;

      // Build merged orderbooks with platform tags
      const mergedBids = [
        ...pmBids.map(b => ({ ...b, platform: "polymarket" })),
        ...kBids.map(b => ({ ...b, platform: "kalshi" })),
      ].sort((a, b) => b.price - a.price);

      const mergedAsks = [
        ...pmAsks.map(a => ({ ...a, platform: "polymarket" })),
        ...kAsks.map(a => ({ ...a, platform: "kalshi" })),
      ].sort((a, b) => a.price - b.price);

      const costUp = computeCostToMoveUp5Cents(mergedAsks);
      const costDown = computeCostToMoveDown5Cents(mergedBids);

      const combined = {
        ticker,
        bellwether_price: tieredPrice.price,
        price_tier: tieredPrice.tier,
        price_label: tieredPrice.label,
        price_source: tieredPrice.source,
        robustness: {
          cost_to_move_5c: minCost,
          cost_to_move_up_5c: costUp,
          cost_to_move_down_5c: costDown,
          reportability,
          raw_reportability: rawReportability,
          weakest_platform: weakestPlatform,
        },
        vwap_details: {
          window_hours: tieredPrice.window_hours,
          trade_count: tieredPrice.trade_count,
          total_volume: tieredPrice.total_volume,
        },
        orderbook_midpoint: computeOrderbookMidpoint(mergedBids, mergedAsks),
        orderbook_summary: {
          bid_levels: mergedBids.length,
          ask_levels: mergedAsks.length,
          best_bid: mergedBids.length > 0 ? mergedBids[0].price : null,
          best_ask: mergedAsks.length > 0 ? mergedAsks[0].price : null,
          top_5_bids: mergedBids.slice(0, 5).map(b => ({ price: b.price, size: b.size, platform: b.platform })),
          top_5_asks: mergedAsks.slice(0, 5).map(a => ({ price: a.price, size: a.size, platform: a.platform })),
        },
        fetched_at: new Date().toISOString(),
      };

      return new Response(JSON.stringify(combined), { headers: corsHeaders });
    }

    // =================================================================
    // MARKETS API — /api/markets/search and /api/markets/top
    // =================================================================

    if (url.pathname === "/api/markets/search" || url.pathname === "/api/markets/top") {
      // Optional API key check (only for /api/markets/* routes)
      const apiKeyResult = checkOptionalApiKey(request, env);
      if (apiKeyResult) {
        return new Response(JSON.stringify(apiKeyResult.body), {
          status: apiKeyResult.status,
          headers: corsHeaders,
        });
      }

      // Category validation (shared by both endpoints)
      const category = url.searchParams.get("category") || null;
      if (category && !VALID_CATEGORIES.includes(category)) {
        return new Response(
          JSON.stringify({
            error: "invalid_category",
            message: "Invalid category. Valid values: " + VALID_CATEGORIES.join(", "),
            valid_categories: VALID_CATEGORIES,
          }),
          { status: 400, headers: corsHeaders }
        );
      }

      // Limit validation (shared by both endpoints)
      const limitParam = url.searchParams.get("limit");
      let limit = 10;
      if (limitParam !== null) {
        limit = parseInt(limitParam, 10);
        if (isNaN(limit) || limit < 1) limit = 10;
        if (limit > 50) {
          return new Response(
            JSON.stringify({
              error: "limit_exceeded",
              message: "Maximum limit is 50",
            }),
            { status: 400, headers: corsHeaders }
          );
        }
      }

      // Load active markets from KV
      let activeData;
      try {
        activeData = await loadActiveMarkets(kv);
      } catch (err) {
        return new Response(
          JSON.stringify({
            error: "data_unavailable",
            message: "Market data is temporarily unavailable",
          }),
          { status: 503, headers: corsHeaders }
        );
      }

      if (!activeData) {
        return new Response(
          JSON.stringify({
            error: "data_unavailable",
            message: "Market data is temporarily unavailable",
          }),
          { status: 503, headers: corsHeaders }
        );
      }

      let filtered = activeData;

      // Filter by category if provided
      if (category) {
        filtered = filtered.filter(m => extractCategory(m.category) === category);
      }

      // --- SEARCH endpoint ---
      if (url.pathname === "/api/markets/search") {
        const q = url.searchParams.get("q");
        if (!q || q.trim() === "") {
          return new Response(
            JSON.stringify({
              error: "empty_query",
              message: "Search query cannot be empty",
            }),
            { status: 400, headers: corsHeaders }
          );
        }

        const queryLower = q.toLowerCase();
        filtered = filtered.filter(m => {
          const ticker = (m.ticker || "").toLowerCase();
          const label = (m.label || "").toLowerCase();
          return ticker.includes(queryLower) || label.includes(queryLower);
        });

        filtered.sort((a, b) => (b.total_volume || 0) - (a.total_volume || 0));
        const total = filtered.length;
        const results = filtered.slice(0, limit).map(formatMarketResult);

        return new Response(
          JSON.stringify({
            results,
            total,
            query: q,
            category: category,
          }),
          { headers: corsHeaders }
        );
      }

      // --- TOP endpoint ---
      if (url.pathname === "/api/markets/top") {
        const totalActive = filtered.length;
        filtered.sort((a, b) => (b.total_volume || 0) - (a.total_volume || 0));
        const results = filtered.slice(0, limit).map(formatMarketResult);

        return new Response(
          JSON.stringify({
            results,
            category: category,
            total_active: totalActive,
          }),
          { headers: corsHeaders }
        );
      }
    }

    return new Response(
      JSON.stringify({
        error: "Not found",
        available_endpoints: ["/", "/health", "/api/markets/BWR-DEM-WIN-SENATE_TX-CERTIFIED-ANY-2026", "/api/markets/search", "/api/markets/top", "/api/metrics/:platform/:token_id", "/api/metrics/combined", "/api/metrics/event/:slug"]
      }),
      { status: 404, headers: corsHeaders }
    );
  },
};
