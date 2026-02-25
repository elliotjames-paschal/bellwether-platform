/**
 * Bellwether Commercial API
 *
 * Live prediction market data for news organizations.
 * - VWAP (Volume-Weighted Average Price)
 * - Reportability metrics (manipulation resistance)
 * - Cross-platform aggregation (Polymarket + Kalshi)
 *
 * Requires API key authentication.
 * Shares cache with internal market monitor for efficiency.
 */

// Native APIs - no authentication required for public data
const KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2";
const POLYMARKET_CLOB_BASE = "https://clob.polymarket.com";

const CONFIG = {
  cache_ttl_ms: 30000, // 30 seconds for price data
  min_trades_for_vwap: 10,
  vwap_windows: [6, 12, 24],
};

// =============================================================================
// MARKET MAPPING - Loaded from KV
// =============================================================================

// Cache for market map data (per-isolate)
let cachedMarketMap = null;
let cacheTimestamp = 0;
const MARKET_MAP_CACHE_TTL = 300000; // 5 minutes

const MARKET_MAP_URL = "https://bellwethermetrics.com/data/market_map.json";

async function loadMarketMap(env) {
  const now = Date.now();

  // Return cached if fresh
  if (cachedMarketMap && (now - cacheTimestamp) < MARKET_MAP_CACHE_TTL) {
    return cachedMarketMap;
  }

  // Fetch from website
  try {
    const response = await fetch(MARKET_MAP_URL);
    if (response.ok) {
      const data = await response.json();
      if (data && data.markets) {
        cachedMarketMap = data.markets;
        cacheTimestamp = now;
        return cachedMarketMap;
      }
    }
  } catch (err) {
    console.error("Failed to fetch market_map:", err);
  }

  // Return stale cache if fetch failed
  if (cachedMarketMap) {
    return cachedMarketMap;
  }

  // Fallback to empty array
  cachedMarketMap = [];
  cacheTimestamp = now;
  return cachedMarketMap;
}

function searchMarketMap(markets, query) {
  const q = query.toLowerCase();
  return markets.filter(m =>
    (m.title && m.title.toLowerCase().includes(q)) ||
    (m.slug && m.slug.toLowerCase().includes(q)) ||
    (m.k_ticker && m.k_ticker.toLowerCase().includes(q)) ||
    (m.category && m.category.toLowerCase().includes(q)) ||
    (m.country && m.country.toLowerCase().includes(q))
  );
}

function getMarketBySlug(markets, slug) {
  return markets.find(m => m.slug === slug);
}

// =============================================================================
// AUTHENTICATION
// =============================================================================

async function validateApiKey(request, env) {
  const authHeader = request.headers.get("Authorization");
  const apiKeyParam = new URL(request.url).searchParams.get("api_key");

  const apiKey = authHeader?.replace("Bearer ", "") || apiKeyParam;

  if (!apiKey) {
    return { valid: false, error: "Missing API key. Include 'Authorization: Bearer <key>' header." };
  }

  if (!env.API_KEYS) {
    // Fallback for development - accept any key starting with "bw_test_"
    if (apiKey.startsWith("bw_test_")) {
      return { valid: true, client: "test_client", tier: "test" };
    }
    return { valid: false, error: "API key validation unavailable" };
  }

  const clientData = await env.API_KEYS.get(apiKey, { type: "json" });

  if (!clientData) {
    return { valid: false, error: "Invalid API key" };
  }

  // Check if key is active
  if (clientData.status !== "active") {
    return { valid: false, error: "API key is inactive" };
  }

  return { valid: true, ...clientData };
}

async function trackUsage(env, clientId, endpoint) {
  if (!env.API_KEYS) return;

  const today = new Date().toISOString().split("T")[0];
  const usageKey = `usage:${clientId}:${today}`;

  try {
    const current = await env.API_KEYS.get(usageKey, { type: "json" }) || { calls: 0, endpoints: {} };
    current.calls += 1;
    current.endpoints[endpoint] = (current.endpoints[endpoint] || 0) + 1;
    current.last_call = new Date().toISOString();

    await env.API_KEYS.put(usageKey, JSON.stringify(current), {
      expirationTtl: 90 * 24 * 60 * 60, // Keep 90 days
    });
  } catch (err) {
    console.error("Usage tracking error:", err);
  }
}

// =============================================================================
// NATIVE API FUNCTIONS (Kalshi Elections API + Polymarket CLOB)
// =============================================================================

async function fetchKalshiOrderbook(ticker) {
  // Kalshi Elections API: GET /markets/{ticker}/orderbook
  const url = `${KALSHI_API_BASE}/markets/${ticker}/orderbook`;

  try {
    const response = await fetch(url, { headers: { "Accept": "application/json" } });
    if (!response.ok) return null;

    const data = await response.json();
    const orderbook = data.orderbook || data;
    const bids = [];
    const asks = [];

    // Kalshi returns yes/no arrays of [price, quantity] pairs
    // Price is in cents (0-100)
    const yesOrders = orderbook.yes || [];
    const noOrders = orderbook.no || [];

    // YES bids = people willing to buy YES at this price
    for (const [priceVal, qty] of yesOrders) {
      const price = Number(priceVal) / 100; // Convert cents to decimal
      const size = Number(qty);
      if (price > 0 && size > 0) bids.push({ price, size });
    }

    // NO orders convert to YES asks: ask price = 1 - no_price
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
  // Polymarket CLOB API: GET /book?token_id={token_id}
  const url = `${POLYMARKET_CLOB_BASE}/book?token_id=${tokenId}`;

  try {
    const response = await fetch(url, { headers: { "Accept": "application/json" } });
    if (!response.ok) return null;

    const data = await response.json();
    const bids = [];
    const asks = [];

    // Polymarket returns bids and asks arrays with price/size
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
  // Kalshi Elections API: GET /markets/trades?ticker={ticker}&min_ts={start}&max_ts={end}
  const nowSec = Math.floor(Date.now() / 1000);
  const startSec = nowSec - (windowHours * 60 * 60);

  const allTrades = [];
  let cursor = null;
  const limit = 1000; // Kalshi allows up to 1000 per page
  const maxPages = 10;

  for (let page = 0; page < maxPages; page++) {
    const params = new URLSearchParams({
      ticker: ticker,
      limit: limit.toString(),
      min_ts: startSec.toString(),
      max_ts: (nowSec + 1).toString(), // max_ts is exclusive
    });
    if (cursor) params.set("cursor", cursor);

    const url = `${KALSHI_API_BASE}/markets/trades?${params}`;

    try {
      const response = await fetch(url, { headers: { "Accept": "application/json" } });
      if (!response.ok) break;

      const data = await response.json();
      const tradeList = data.trades || [];

      for (const trade of tradeList) {
        // Price is in cents (0-100), convert to decimal
        const price = Number(trade.yes_price) / 100;
        const size = Number(trade.count || 1);
        let timestamp = trade.created_time;

        // Parse ISO timestamp to milliseconds
        if (typeof timestamp === "string") {
          timestamp = new Date(timestamp).getTime();
        } else if (timestamp < 1e12) {
          timestamp = timestamp * 1000;
        }

        if (price > 0 && price <= 1) {
          allTrades.push({ price, size, timestamp });
        }
      }

      // Check for more pages
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
  // Polymarket CLOB API: GET /trades?asset_id={token_id}
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
      const response = await fetch(url, { headers: { "Accept": "application/json" } });
      if (!response.ok) break;

      const data = await response.json();
      const tradeList = Array.isArray(data) ? data : (data.trades || data.data || []);

      for (const trade of tradeList) {
        const price = Number(trade.price || trade.p);
        const size = Number(trade.size || trade.s || trade.amount || 1);
        let timestamp = Number(trade.timestamp || trade.t || trade.time || trade.created_at);
        if (timestamp < 1e12) timestamp = timestamp * 1000;

        // Only include trades within our time window
        if (price > 0 && price <= 1 && timestamp >= startMs) {
          allTrades.push({ price, size, timestamp });
        }
      }

      // Check for pagination
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
// CALCULATIONS
// =============================================================================

function computeVWAP(trades) {
  if (trades.length === 0) return { vwap: null, trade_count: 0, total_volume: 0 };

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

function computeCostToMove5Cents(bids, asks) {
  const costUp = computeCostToMoveUp5Cents(asks);
  const costDown = computeCostToMoveDown5Cents(bids);

  if (costUp === null && costDown === null) return null;
  if (costUp === null) return costDown;
  if (costDown === null) return costUp;
  return Math.min(costUp, costDown);
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

function getReportability(costToMove5c, tier) {
  let base;
  if (costToMove5c === null || costToMove5c < 10000) base = "fragile";
  else if (costToMove5c < 100000) base = "caution";
  else base = "robust";

  // Downgrade for stale data
  if (tier === 2) {
    if (base === "robust") return "caution";
    if (base === "caution") return "fragile";
  }
  if (tier === 3) return "fragile";

  return base;
}

// =============================================================================
// CACHE FUNCTIONS
// =============================================================================

async function getCached(kv, key) {
  if (!kv) return null;
  try {
    const cached = await kv.get(key, { type: "json" });
    if (!cached) return null;
    if (Date.now() - new Date(cached.fetched_at).getTime() > CONFIG.cache_ttl_ms) return null;
    return { ...cached, cached: true };
  } catch (err) {
    return null;
  }
}

async function setCache(kv, key, data) {
  if (!kv) return;
  try {
    await kv.put(key, JSON.stringify(data), {
      expirationTtl: Math.ceil(CONFIG.cache_ttl_ms / 1000) + 60,
    });
  } catch (err) {
    console.error("Cache write error:", err);
  }
}

// =============================================================================
// MAIN DATA FETCHING
// =============================================================================

async function getMarketData(platform, tokenId, env) {
  const cacheKey = `${platform}:${tokenId}`;
  const cached = await getCached(env.BELLWETHER_KV, cacheKey);
  if (cached) return cached;

  // Native APIs don't require authentication
  const orderbook = await fetchOrderbook(platform, tokenId);
  if (!orderbook) return null;

  const [bids, asks] = orderbook;
  const allTrades = await fetchTrades(platform, tokenId, 24);
  const now = Date.now();

  // Find best VWAP window
  let vwapResult = null;
  let windowHours = null;
  let tier = 3;

  for (const hours of CONFIG.vwap_windows) {
    const cutoff = now - (hours * 60 * 60 * 1000);
    const windowTrades = allTrades.filter(t => t.timestamp >= cutoff);
    const result = computeVWAP(windowTrades);

    if (result.trade_count >= CONFIG.min_trades_for_vwap) {
      vwapResult = result;
      windowHours = hours;
      tier = hours === 6 ? 1 : 2;
      break;
    }
  }

  if (!vwapResult) {
    vwapResult = computeVWAP(allTrades);
  }

  const costToMove5c = computeCostToMove5Cents(bids, asks);
  const reportability = getReportability(costToMove5c, tier);

  const midpoint = (bids.length > 0 && asks.length > 0)
    ? Math.round(((bids[0].price + asks[0].price) / 2) * 10000) / 10000
    : null;

  const data = {
    market_id: tokenId,
    platform,
    price: vwapResult.vwap || midpoint,
    vwap: vwapResult.vwap,
    vwap_window_hours: windowHours,
    vwap_trade_count: vwapResult.trade_count,
    vwap_volume: vwapResult.total_volume,  // Dollar volume for Duffie-style weighting
    midpoint,
    reportability,
    cost_to_move_5c: costToMove5c,
    tier,
    fetched_at: new Date().toISOString(),
    cached: false,
  };

  await setCache(env.BELLWETHER_KV, cacheKey, data);
  return data;
}

async function getCombinedMarketData(pmToken, kTicker, env) {
  const cacheKey = `combined:${pmToken || ""}:${kTicker || ""}`;
  const cached = await getCached(env.BELLWETHER_KV, cacheKey);
  if (cached) return cached;

  // Fetch both platforms in parallel
  const [pmData, kData] = await Promise.all([
    pmToken ? getMarketData("polymarket", pmToken, env) : null,
    kTicker ? getMarketData("kalshi", kTicker, env) : null,
  ]);

  // Compute combined VWAP weighted by dollar volume (per Duffie & Dworczak 2021)
  // Formula: Σ(VWAP_i × Volume_i) / Σ(Volume_i)
  let combinedPrice = null;
  let totalVolume = 0;

  if (pmData?.vwap && pmData.vwap_volume > 0) {
    combinedPrice = (combinedPrice || 0) + pmData.vwap * pmData.vwap_volume;
    totalVolume += pmData.vwap_volume;
  }
  if (kData?.vwap && kData.vwap_volume > 0) {
    combinedPrice = (combinedPrice || 0) + kData.vwap * kData.vwap_volume;
    totalVolume += kData.vwap_volume;
  }
  if (totalVolume > 0) {
    combinedPrice = Math.round((combinedPrice / totalVolume) * 10000) / 10000;
  } else {
    combinedPrice = pmData?.midpoint || kData?.midpoint || null;
  }

  // Use minimum cost (weakest link)
  const pmCost = pmData?.cost_to_move_5c;
  const kCost = kData?.cost_to_move_5c;
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

  const tier = Math.max(pmData?.tier || 3, kData?.tier || 3);
  const reportability = getReportability(minCost, tier);

  const data = {
    price: combinedPrice,
    total_volume: totalVolume,
    platforms: {
      polymarket: pmData ? {
        price: pmData.vwap || pmData.midpoint,
        volume: pmData.vwap_volume || 0,
        cost_to_move_5c: pmData.cost_to_move_5c,
      } : null,
      kalshi: kData ? {
        price: kData.vwap || kData.midpoint,
        volume: kData.vwap_volume || 0,
        cost_to_move_5c: kData.cost_to_move_5c,
      } : null,
    },
    reportability,
    cost_to_move_5c: minCost,
    weakest_platform: weakestPlatform,
    tier,
    fetched_at: new Date().toISOString(),
    cached: false,
  };

  await setCache(env.BELLWETHER_KV, cacheKey, data);
  return data;
}

// =============================================================================
// HTTP HANDLER
// =============================================================================

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Content-Type": "application/json",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // Public endpoints (no auth required)
    if (url.pathname === "/" || url.pathname === "/v1") {
      return new Response(JSON.stringify({
        name: "Bellwether Commercial API",
        version: "1.0.0",
        description: "Live prediction market data for news organizations",
        documentation: "https://bellwethermetrics.com/api-docs",
        endpoints: {
          "GET /v1/events": "List all available cross-platform events",
          "GET /v1/search?q=query": "Search events by keyword",
          "GET /v1/events/:slug": "Get aggregated data by Bellwether event slug",
          "GET /v1/markets/:platform/:market_id": "Get data for a specific platform market",
          "GET /v1/health": "API health check",
        },
        authentication: "Include 'Authorization: Bearer <api_key>' header",
      }), { headers: corsHeaders });
    }

    if (url.pathname === "/v1/health" || url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        cache_ttl_seconds: CONFIG.cache_ttl_ms / 1000,
        timestamp: new Date().toISOString(),
      }), { headers: corsHeaders });
    }

    // All other endpoints require authentication
    const auth = await validateApiKey(request, env);
    if (!auth.valid) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: auth.error,
      }), { status: 401, headers: corsHeaders });
    }

    // Track usage
    ctx.waitUntil(trackUsage(env, auth.client, url.pathname));

    // GET /v1/events - List all available cross-platform events
    if (url.pathname === "/v1/events") {
      const markets = await loadMarketMap(env);
      const limit = Math.min(parseInt(url.searchParams.get("limit") || "50"), 200);
      const offset = parseInt(url.searchParams.get("offset") || "0");
      const category = url.searchParams.get("category");
      const country = url.searchParams.get("country");

      let filtered = markets;
      if (category) {
        filtered = filtered.filter(m => m.category && m.category.toLowerCase() === category.toLowerCase());
      }
      if (country) {
        filtered = filtered.filter(m => m.country && m.country.toLowerCase() === country.toLowerCase());
      }

      const paginated = filtered.slice(offset, offset + limit);

      return new Response(JSON.stringify({
        total: filtered.length,
        limit,
        offset,
        markets: paginated.map(m => ({
          slug: m.slug,
          title: m.title,
          k_ticker: m.k_ticker,
          pm_token: m.pm_token,
          category: m.category,
          country: m.country,
          total_volume: m.total_volume,
        })),
      }), { headers: corsHeaders });
    }

    // GET /v1/search?q=query
    if (url.pathname === "/v1/search") {
      const query = url.searchParams.get("q");

      if (!query || query.trim().length < 2) {
        return new Response(JSON.stringify({
          error: "Invalid query",
          message: "Provide a search query with at least 2 characters: ?q=trump",
        }), { status: 400, headers: corsHeaders });
      }

      const markets = await loadMarketMap(env);
      const results = searchMarketMap(markets, query.trim());

      return new Response(JSON.stringify({
        query: query.trim(),
        count: results.length,
        markets: results.map(m => ({
          slug: m.slug,
          title: m.title,
          k_ticker: m.k_ticker,
          pm_token: m.pm_token,
          category: m.category,
          country: m.country,
          platforms: [m.k_ticker ? "kalshi" : null, m.pm_token ? "polymarket" : null].filter(Boolean),
        })),
      }), { headers: corsHeaders });
    }

    // GET /v1/events/:slug - Get market data by Bellwether slug
    const slugMatch = url.pathname.match(/^\/v1\/events\/([a-z0-9-]+)$/);
    if (slugMatch) {
      const slug = slugMatch[1];
      const markets = await loadMarketMap(env);
      const market = getMarketBySlug(markets, slug);

      if (!market) {
        return new Response(JSON.stringify({
          error: "Event not found",
          hint: "Use /v1/search?q=query to find available events",
          example_slugs: markets.slice(0, 10).map(m => m.slug),
        }), { status: 404, headers: corsHeaders });
      }

      const data = await getCombinedMarketData(market.pm_token_id || market.pm_token, market.k_ticker, env);
      if (!data) {
        return new Response(JSON.stringify({
          error: "Market data unavailable",
        }), { status: 404, headers: corsHeaders });
      }

      return new Response(JSON.stringify({
        slug: market.slug,
        title: market.title,
        ...data,
      }), { headers: corsHeaders });
    }

    // GET /v1/markets/combined?pm_token=X&k_ticker=Y
    if (url.pathname === "/v1/markets/combined") {
      const pmToken = url.searchParams.get("pm_token");
      const kTicker = url.searchParams.get("k_ticker");

      if (!pmToken && !kTicker) {
        return new Response(JSON.stringify({
          error: "Missing parameters",
          message: "Provide at least one of: pm_token, k_ticker",
        }), { status: 400, headers: corsHeaders });
      }

      const data = await getCombinedMarketData(pmToken, kTicker, env);
      if (!data) {
        return new Response(JSON.stringify({
          error: "Market not found",
        }), { status: 404, headers: corsHeaders });
      }

      return new Response(JSON.stringify(data), { headers: corsHeaders });
    }

    // GET /v1/markets/:platform/:market_id
    const marketMatch = url.pathname.match(/^\/v1\/markets\/(polymarket|kalshi)\/(.+)$/);
    if (marketMatch) {
      const platform = marketMatch[1];
      const marketId = marketMatch[2];

      const data = await getMarketData(platform, marketId, env);
      if (!data) {
        return new Response(JSON.stringify({
          error: "Market not found",
          hint: "Check that the market_id is valid",
        }), { status: 404, headers: corsHeaders });
      }

      return new Response(JSON.stringify(data), { headers: corsHeaders });
    }

    return new Response(JSON.stringify({
      error: "Not found",
      endpoints: ["/v1/search?q=query", "/v1/markets/:platform/:market_id", "/v1/markets/combined"],
    }), { status: 404, headers: corsHeaders });
  },
};
