/**
 * Bellwether Live Data Server
 *
 * Lightweight caching layer that:
 * 1. Maintains ONE WebSocket connection to Dome API (for trades)
 * 2. Polls orderbook data every 30-60 seconds
 * 3. Computes manipulation cost and 6-hour VWAP
 * 4. Serves computed metrics to website visitors
 *
 * Can run on: Cloudflare Workers, Deno Deploy, Fly.io, Render,
 *             Stanford Farmshare, or any Node.js host
 */

const DOME_API_KEY = process.env.DOME_API_KEY;
const DOME_WS_URL = `wss://ws.domeapi.io/${DOME_API_KEY}`;
const DOME_REST_BASE = 'https://api.domeapi.io/v1';

// Configuration
const CONFIG = {
    // ==========================================================================
    // TIERED POLLING STRATEGY
    // ==========================================================================
    // You have 10,210 markets but Dome Dev tier = 100 calls/sec
    // Solution: Poll high-value markets frequently, others less/not at all
    //
    tiers: {
        // Tier 1: Featured markets (homepage highlights)
        tier1: {
            max_markets: 50,
            orderbook_interval_ms: 60000,   // Every 60 seconds
            trades_interval_ms: 60000,      // Every 60 seconds
        },
        // Tier 2: Active markets (visible in market monitor)
        tier2: {
            max_markets: 500,
            orderbook_interval_ms: 300000,  // Every 5 minutes
            trades_interval_ms: 300000,     // Every 5 minutes
        },
        // Tier 3: Everything else - use daily pipeline data, no live polling
    },

    // Cache TTL (how long data is valid for website visitors)
    cache_ttl_ms: 30000,                // Serve cached data for 30 seconds

    // Feature settings
    vwap_window_hours: 6,               // 6-hour VWAP per Duffie method
    manipulation_test_amount: 100000,   // $100K test amount

    // Markets (populated at startup from active_markets.json)
    tier1_markets: [],  // Top 50 by volume
    tier2_markets: [],  // Next 500 by volume
};

/*
 * API CALL BUDGET (with tiered approach):
 *
 * LIVE SERVER:
 *   Tier 1: 50 markets × 2 calls/min (orderbook + trades) = 100 calls/min
 *   Tier 2: 500 markets × 0.4 calls/min = 200 calls/min
 *   Live total: ~300 calls/min = 5 calls/sec
 *
 * DAILY PIPELINE (same API key!):
 *   Runs once per day, ~10K markets
 *   Burst during pipeline: ~50-100 calls/sec for ~30 min
 *
 * Dome Dev tier: 100 calls/sec
 *
 * STRATEGY:
 *   - Live server uses ~5 calls/sec (5% of budget)
 *   - Pipeline runs daily, bursts but finishes quickly
 *   - Both can coexist safely with 95% headroom
 *   - If pipeline is running, live server continues fine
 *
 * Website visitors:
 *   - Unlimited users can hit /metrics
 *   - All served from cache = 0 extra API calls
 */

// In-memory cache
const cache = {
    orderbooks: {},          // token_id -> { bids: [], asks: [], timestamp }
    trades: {},              // token_id -> [{ price, size, timestamp }, ...]
    computed_metrics: {},    // token_id -> { manipulation_cost, vwap_6h, updated_at }
};

// =============================================================================
// ADAPTIVE RATE LIMITING
// =============================================================================
// Self-regulating: backs off when rate limited, restores when clear
//
const rateLimiter = {
    currentDelayMs: 50,       // Current delay between API calls
    minDelayMs: 50,           // Normal operation: 50ms = 20 calls/sec max
    maxDelayMs: 5000,         // Max backoff: 5 seconds between calls
    backoffMultiplier: 2,     // Double delay on rate limit
    restoreMultiplier: 0.9,   // Slowly restore (10% faster each success)
    consecutiveSuccesses: 0,
    consecutiveFailures: 0,
    isBackingOff: false,
};

function recordApiSuccess() {
    rateLimiter.consecutiveSuccesses++;
    rateLimiter.consecutiveFailures = 0;

    // After 10 consecutive successes, start restoring speed
    if (rateLimiter.consecutiveSuccesses > 10 && rateLimiter.currentDelayMs > rateLimiter.minDelayMs) {
        rateLimiter.currentDelayMs = Math.max(
            rateLimiter.minDelayMs,
            rateLimiter.currentDelayMs * rateLimiter.restoreMultiplier
        );
        if (rateLimiter.isBackingOff) {
            console.log(`[RateLimit] Restoring speed, delay now ${Math.round(rateLimiter.currentDelayMs)}ms`);
        }
        rateLimiter.isBackingOff = false;
    }
}

function recordApiRateLimit() {
    rateLimiter.consecutiveFailures++;
    rateLimiter.consecutiveSuccesses = 0;
    rateLimiter.isBackingOff = true;

    // Exponential backoff
    rateLimiter.currentDelayMs = Math.min(
        rateLimiter.maxDelayMs,
        rateLimiter.currentDelayMs * rateLimiter.backoffMultiplier
    );
    console.log(`[RateLimit] Backing off, delay now ${Math.round(rateLimiter.currentDelayMs)}ms`);
}

async function rateLimitedDelay() {
    await new Promise(r => setTimeout(r, rateLimiter.currentDelayMs));
}

// ============================================================================
// MANIPULATION COST CALCULATION
// ============================================================================

/**
 * Simulate a buy of $X and compute how much the price moves.
 *
 * @param {Array} asks - Array of [price, size] tuples, sorted by price ascending
 * @param {number} currentMidpoint - Current midpoint price (0-1)
 * @param {number} dollarAmount - Amount to simulate buying (e.g., 100000 for $100K)
 * @returns {object} { price_impact_cents, volume_consumed, levels_consumed }
 */
function computeManipulationCost(asks, currentMidpoint, dollarAmount) {
    if (!asks || asks.length === 0) {
        return { price_impact_cents: null, volume_consumed: 0, levels_consumed: 0 };
    }

    // Sort asks by price (lowest first)
    const sortedAsks = [...asks].sort((a, b) => a[0] - b[0]);

    let remainingDollars = dollarAmount;
    let volumeConsumed = 0;
    let levelsConsumed = 0;
    let lastPrice = currentMidpoint;

    for (const [price, size] of sortedAsks) {
        if (remainingDollars <= 0) break;

        // Cost to consume this level = price * size
        const levelCost = price * size;

        if (levelCost <= remainingDollars) {
            // Consume entire level
            remainingDollars -= levelCost;
            volumeConsumed += size;
            levelsConsumed++;
            lastPrice = price;
        } else {
            // Partial consumption of this level
            const sharesToBuy = remainingDollars / price;
            volumeConsumed += sharesToBuy;
            levelsConsumed++;
            lastPrice = price;
            remainingDollars = 0;
        }
    }

    // Price impact = new price - old midpoint (in cents, assuming prices are 0-1)
    const priceImpactCents = (lastPrice - currentMidpoint) * 100;

    return {
        price_impact_cents: Math.round(priceImpactCents * 100) / 100,  // Round to 2 decimals
        volume_consumed: Math.round(volumeConsumed),
        levels_consumed: levelsConsumed,
        dollars_spent: dollarAmount - remainingDollars,
    };
}

// ============================================================================
// 6-HOUR VWAP CALCULATION (Duffie Method)
// ============================================================================

/**
 * Compute volume-weighted average price over the past N hours.
 *
 * @param {Array} trades - Array of { price, size, timestamp } objects
 * @param {number} windowHours - Number of hours to look back (default 6)
 * @returns {number|null} VWAP or null if no trades
 */
function computeVWAP(trades, windowHours = 6) {
    if (!trades || trades.length === 0) return null;

    const cutoffTime = Date.now() - (windowHours * 60 * 60 * 1000);
    const recentTrades = trades.filter(t => t.timestamp >= cutoffTime);

    if (recentTrades.length === 0) return null;

    let sumPriceVolume = 0;
    let sumVolume = 0;

    for (const trade of recentTrades) {
        sumPriceVolume += trade.price * trade.size;
        sumVolume += trade.size;
    }

    if (sumVolume === 0) return null;

    return sumPriceVolume / sumVolume;
}

// ============================================================================
// DOME API INTEGRATION
// ============================================================================

/**
 * Fetch current orderbook from Dome REST API
 * Uses adaptive rate limiting - backs off when rate limited
 */
async function fetchOrderbook(platform, tokenId) {
    const endpoint = platform === 'polymarket'
        ? `${DOME_REST_BASE}/polymarket/orderbooks`
        : `${DOME_REST_BASE}/kalshi/orderbooks`;

    const params = new URLSearchParams({
        [platform === 'polymarket' ? 'token_id' : 'ticker']: tokenId,
        limit: 1,  // Just get latest snapshot
    });

    try {
        const response = await fetch(`${endpoint}?${params}`, {
            headers: { 'Authorization': DOME_API_KEY }
        });

        // Handle rate limiting
        if (response.status === 429) {
            recordApiRateLimit();
            return null;
        }

        if (!response.ok) {
            console.error(`Orderbook fetch failed: ${response.status}`);
            return null;
        }

        // Success - record it for adaptive throttling
        recordApiSuccess();

        const data = await response.json();
        const snapshots = data.snapshots || [];

        if (snapshots.length === 0) return null;

        const latest = snapshots[0];
        return {
            bids: latest.orderbook?.yes || latest.bids || [],
            asks: latest.orderbook?.no || latest.asks || [],
            timestamp: latest.timestamp,
            midpoint: latest.midpoint || null,
        };
    } catch (err) {
        console.error(`Orderbook fetch error: ${err.message}`);
        recordApiRateLimit();  // Treat errors as potential rate limits
        return null;
    }
}

/**
 * Fetch recent trades/candlesticks from Dome REST API
 * Uses adaptive rate limiting - backs off when rate limited
 */
async function fetchRecentTrades(platform, tokenId, hoursBack = 6) {
    const endpoint = platform === 'polymarket'
        ? `${DOME_REST_BASE}/polymarket/candlesticks/${tokenId}`
        : `${DOME_REST_BASE}/kalshi/candlesticks/${tokenId}`;

    const endTime = Date.now();
    const startTime = endTime - (hoursBack * 60 * 60 * 1000);

    const params = new URLSearchParams({
        interval: 60,  // 1-hour candles (or smallest available)
        start_time: startTime,
        end_time: endTime,
    });

    try {
        const response = await fetch(`${endpoint}?${params}`, {
            headers: { 'Authorization': DOME_API_KEY }
        });

        // Handle rate limiting
        if (response.status === 429) {
            recordApiRateLimit();
            return null;
        }

        if (!response.ok) {
            console.error(`Trades fetch failed: ${response.status}`);
            return null;
        }

        // Success - record it for adaptive throttling
        recordApiSuccess();

        const data = await response.json();
        // Convert candlesticks to trade-like format for VWAP calculation
        // Each candle has: open, high, low, close, volume, timestamp
        const candles = data.candlesticks || data.candles || [];

        return candles.map(c => ({
            price: c.close || c.c || c.p,
            size: c.volume || c.v || 1,
            timestamp: c.timestamp || c.t,
        }));
    } catch (err) {
        console.error(`Trades fetch error: ${err.message}`);
        recordApiRateLimit();  // Treat errors as potential rate limits
        return null;
    }
}

// ============================================================================
// WEBSOCKET CONNECTION TO DOME
// ============================================================================

let ws = null;
let wsReconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;

function connectWebSocket() {
    if (!DOME_API_KEY) {
        console.error('DOME_API_KEY not set');
        return;
    }

    console.log('Connecting to Dome WebSocket...');

    ws = new WebSocket(DOME_WS_URL);

    ws.onopen = () => {
        console.log('WebSocket connected');
        wsReconnectAttempts = 0;

        // Subscribe to trades for featured markets
        for (const market of CONFIG.featured_markets) {
            const subscribeMsg = {
                action: 'subscribe',
                platform: market.platform,
                version: 1,
                type: 'orders',
                filters: {
                    condition_ids: [market.condition_id]
                }
            };
            ws.send(JSON.stringify(subscribeMsg));
        }
    };

    ws.onmessage = (event) => {
        try {
            const msg = JSON.parse(event.data);
            handleTradeMessage(msg);
        } catch (err) {
            console.error('WebSocket message parse error:', err);
        }
    };

    ws.onclose = () => {
        console.log('WebSocket disconnected');
        scheduleReconnect();
    };

    ws.onerror = (err) => {
        console.error('WebSocket error:', err);
    };
}

function scheduleReconnect() {
    if (wsReconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
        console.error('Max reconnection attempts reached');
        return;
    }

    const delay = Math.min(1000 * Math.pow(2, wsReconnectAttempts), 30000);
    wsReconnectAttempts++;

    console.log(`Reconnecting in ${delay}ms (attempt ${wsReconnectAttempts})`);
    setTimeout(connectWebSocket, delay);
}

function handleTradeMessage(msg) {
    // Handle incoming trade from WebSocket
    // Format depends on Dome's message structure
    if (msg.type === 'order' || msg.type === 'trade') {
        const tokenId = msg.token_id || msg.condition_id;
        if (!cache.trades[tokenId]) {
            cache.trades[tokenId] = [];
        }

        cache.trades[tokenId].push({
            price: msg.price,
            size: msg.size || msg.amount,
            timestamp: msg.timestamp || Date.now(),
        });

        // Keep only last 6 hours of trades
        const cutoff = Date.now() - (CONFIG.vwap_window_hours * 60 * 60 * 1000);
        cache.trades[tokenId] = cache.trades[tokenId].filter(t => t.timestamp >= cutoff);

        // Recompute VWAP
        updateComputedMetrics(tokenId);
    }
}

// ============================================================================
// POLLING & METRIC COMPUTATION
// ============================================================================

async function pollOrderbooks() {
    console.log(`Polling orderbooks for ${CONFIG.featured_markets.length} markets...`);

    for (const market of CONFIG.featured_markets) {
        const orderbook = await fetchOrderbook(market.platform, market.token_id);

        if (orderbook) {
            cache.orderbooks[market.token_id] = orderbook;

            // Compute manipulation cost
            const manipResult = computeManipulationCost(
                orderbook.asks,
                orderbook.midpoint || 0.5,
                CONFIG.manipulation_test_amount
            );

            // Update computed metrics
            if (!cache.computed_metrics[market.token_id]) {
                cache.computed_metrics[market.token_id] = {};
            }
            cache.computed_metrics[market.token_id].manipulation_cost = manipResult;
            cache.computed_metrics[market.token_id].updated_at = Date.now();
        }

        // Small delay between requests to avoid rate limiting
        await new Promise(r => setTimeout(r, 100));
    }
}

async function pollTrades() {
    console.log(`Polling trades for ${CONFIG.featured_markets.length} markets...`);

    for (const market of CONFIG.featured_markets) {
        const trades = await fetchRecentTrades(
            market.platform,
            market.token_id,
            CONFIG.vwap_window_hours
        );

        if (trades) {
            cache.trades[market.token_id] = trades;
            updateComputedMetrics(market.token_id);
        }

        await new Promise(r => setTimeout(r, 100));
    }
}

function updateComputedMetrics(tokenId) {
    const trades = cache.trades[tokenId];
    const vwap = computeVWAP(trades, CONFIG.vwap_window_hours);

    if (!cache.computed_metrics[tokenId]) {
        cache.computed_metrics[tokenId] = {};
    }
    cache.computed_metrics[tokenId].vwap_6h = vwap;
    cache.computed_metrics[tokenId].updated_at = Date.now();
}

// ============================================================================
// HTTP SERVER
// ============================================================================

/**
 * Simple HTTP handler - can be adapted for different platforms
 */
function handleRequest(request) {
    const url = new URL(request.url);

    // CORS headers for browser access
    const corsHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Content-Type': 'application/json',
    };

    if (request.method === 'OPTIONS') {
        return new Response(null, { headers: corsHeaders });
    }

    // GET /metrics - Return all computed metrics (from cache)
    if (url.pathname === '/metrics' || url.pathname === '/api/metrics') {
        // This serves cached data - no API call to Dome
        // 1000 users can hit this endpoint, still only 1 call/min to Dome
        const oldestUpdate = Math.min(
            ...Object.values(cache.computed_metrics).map(m => m.updated_at || 0)
        );
        const cacheAgeMs = Date.now() - oldestUpdate;

        return new Response(JSON.stringify({
            generated_at: new Date().toISOString(),
            cache_age_seconds: Math.round(cacheAgeMs / 1000),
            next_refresh_seconds: Math.max(0, Math.round((CONFIG.orderbook_poll_interval_ms - cacheAgeMs) / 1000)),
            manipulation_test_amount: CONFIG.manipulation_test_amount,
            vwap_window_hours: CONFIG.vwap_window_hours,
            markets_count: Object.keys(cache.computed_metrics).length,
            markets: cache.computed_metrics,
        }), { headers: corsHeaders });
    }

    // GET /metrics/:token_id - Return metrics for specific market
    const marketMatch = url.pathname.match(/^\/(?:api\/)?metrics\/(.+)$/);
    if (marketMatch) {
        const tokenId = marketMatch[1];
        const metrics = cache.computed_metrics[tokenId];

        if (!metrics) {
            return new Response(JSON.stringify({ error: 'Market not found' }), {
                status: 404,
                headers: corsHeaders,
            });
        }

        return new Response(JSON.stringify({
            token_id: tokenId,
            ...metrics,
        }), { headers: corsHeaders });
    }

    // GET /health - Health check with rate limiter status
    if (url.pathname === '/health') {
        return new Response(JSON.stringify({
            status: 'ok',
            markets_tracked: {
                tier1: CONFIG.tier1_markets.length,
                tier2: CONFIG.tier2_markets.length,
            },
            cache_size: Object.keys(cache.computed_metrics).length,
            rate_limiter: {
                current_delay_ms: Math.round(rateLimiter.currentDelayMs),
                is_backing_off: rateLimiter.isBackingOff,
                consecutive_successes: rateLimiter.consecutiveSuccesses,
                status: rateLimiter.isBackingOff ? 'backing_off' : 'normal',
            },
        }), { headers: corsHeaders });
    }

    return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: corsHeaders,
    });
}

// ============================================================================
// STARTUP
// ============================================================================

async function loadFeaturedMarkets() {
    console.log('Loading markets from active_markets.json...');

    try {
        // Load active_markets.json (adjust path based on deployment)
        const fs = require('fs');
        const path = require('path');

        // Try multiple possible paths
        const possiblePaths = [
            path.join(__dirname, '../website/data/active_markets.json'),
            path.join(__dirname, './active_markets.json'),
            '/app/data/active_markets.json',  // Docker/container path
        ];

        let data = null;
        for (const p of possiblePaths) {
            if (fs.existsSync(p)) {
                data = JSON.parse(fs.readFileSync(p, 'utf8'));
                console.log(`Loaded markets from ${p}`);
                break;
            }
        }

        if (!data || !data.markets) {
            console.warn('Could not load active_markets.json, using empty config');
            return;
        }

        // Sort by total volume descending
        const sortedMarkets = data.markets
            .filter(m => m.pm_token_id_yes || m.k_ticker)  // Must have tradeable ID
            .sort((a, b) => (b.total_volume || 0) - (a.total_volume || 0));

        // Tier 1: Top 50 by volume
        CONFIG.tier1_markets = sortedMarkets.slice(0, CONFIG.tiers.tier1.max_markets).map(m => ({
            label: m.label,
            platform: m.pm_token_id_yes ? 'polymarket' : 'kalshi',
            token_id: m.pm_token_id_yes || m.k_ticker,
            condition_id: m.pm_condition_id,
            volume: m.total_volume,
        }));

        // Tier 2: Next 500 by volume
        CONFIG.tier2_markets = sortedMarkets
            .slice(CONFIG.tiers.tier1.max_markets, CONFIG.tiers.tier1.max_markets + CONFIG.tiers.tier2.max_markets)
            .map(m => ({
                label: m.label,
                platform: m.pm_token_id_yes ? 'polymarket' : 'kalshi',
                token_id: m.pm_token_id_yes || m.k_ticker,
                condition_id: m.pm_condition_id,
                volume: m.total_volume,
            }));

        console.log(`Tier 1: ${CONFIG.tier1_markets.length} markets (polled every ${CONFIG.tiers.tier1.orderbook_interval_ms/1000}s)`);
        console.log(`Tier 2: ${CONFIG.tier2_markets.length} markets (polled every ${CONFIG.tiers.tier2.orderbook_interval_ms/1000}s)`);
        console.log(`Top market: ${CONFIG.tier1_markets[0]?.label} ($${(CONFIG.tier1_markets[0]?.volume/1e6).toFixed(1)}M volume)`);

    } catch (err) {
        console.error('Error loading markets:', err.message);
    }
}

async function pollTier(tierName, markets, type) {
    if (!markets || markets.length === 0) return;

    console.log(`[${tierName}] Polling ${type} for ${markets.length} markets...`);

    for (const market of markets) {
        try {
            if (type === 'orderbook') {
                const orderbook = await fetchOrderbook(market.platform, market.token_id);
                if (orderbook) {
                    cache.orderbooks[market.token_id] = orderbook;

                    const manipResult = computeManipulationCost(
                        orderbook.asks,
                        orderbook.midpoint || 0.5,
                        CONFIG.manipulation_test_amount
                    );

                    if (!cache.computed_metrics[market.token_id]) {
                        cache.computed_metrics[market.token_id] = { label: market.label };
                    }
                    cache.computed_metrics[market.token_id].manipulation_cost = manipResult;
                    cache.computed_metrics[market.token_id].updated_at = Date.now();
                    cache.computed_metrics[market.token_id].tier = tierName;
                }
            } else if (type === 'trades') {
                const trades = await fetchRecentTrades(
                    market.platform,
                    market.token_id,
                    CONFIG.vwap_window_hours
                );
                if (trades) {
                    cache.trades[market.token_id] = trades;
                    const vwap = computeVWAP(trades, CONFIG.vwap_window_hours);

                    if (!cache.computed_metrics[market.token_id]) {
                        cache.computed_metrics[market.token_id] = { label: market.label };
                    }
                    cache.computed_metrics[market.token_id].vwap_6h = vwap;
                    cache.computed_metrics[market.token_id].updated_at = Date.now();
                }
            }
        } catch (err) {
            console.error(`[${tierName}] Error polling ${market.token_id}:`, err.message);
        }

        // Adaptive rate limiting: delay adjusts based on API responses
        await rateLimitedDelay();
    }
}

async function start() {
    console.log('==========================================');
    console.log('  Bellwether Live Data Server');
    console.log('==========================================');

    await loadFeaturedMarkets();

    // Initial poll for all tiers
    console.log('\nInitial data fetch...');
    await pollTier('tier1', CONFIG.tier1_markets, 'orderbook');
    await pollTier('tier1', CONFIG.tier1_markets, 'trades');
    await pollTier('tier2', CONFIG.tier2_markets, 'orderbook');
    await pollTier('tier2', CONFIG.tier2_markets, 'trades');

    // Set up tiered polling intervals
    // Tier 1: Every 60 seconds
    setInterval(() => pollTier('tier1', CONFIG.tier1_markets, 'orderbook'), CONFIG.tiers.tier1.orderbook_interval_ms);
    setInterval(() => pollTier('tier1', CONFIG.tier1_markets, 'trades'), CONFIG.tiers.tier1.trades_interval_ms);

    // Tier 2: Every 5 minutes
    setInterval(() => pollTier('tier2', CONFIG.tier2_markets, 'orderbook'), CONFIG.tiers.tier2.orderbook_interval_ms);
    setInterval(() => pollTier('tier2', CONFIG.tier2_markets, 'trades'), CONFIG.tiers.tier2.trades_interval_ms);

    console.log('\n==========================================');
    console.log('  Server ready!');
    console.log(`  Tier 1: ${CONFIG.tier1_markets.length} markets @ 60s`);
    console.log(`  Tier 2: ${CONFIG.tier2_markets.length} markets @ 5min`);
    console.log('==========================================\n');
}

// ============================================================================
// PLATFORM-SPECIFIC EXPORTS
// ============================================================================

// For Node.js / Express
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        handleRequest,
        start,
        computeManipulationCost,
        computeVWAP,
        cache,
        CONFIG,
    };
}

// For Cloudflare Workers
if (typeof addEventListener !== 'undefined') {
    addEventListener('fetch', event => {
        event.respondWith(handleRequest(event.request));
    });
}

// For Deno
if (typeof Deno !== 'undefined') {
    Deno.serve(handleRequest);
}

// Auto-start if run directly
if (typeof require !== 'undefined' && require.main === module) {
    const http = require('http');
    start().then(() => {
        const server = http.createServer((req, res) => {
            const request = new Request(`http://localhost${req.url}`, { method: req.method });
            handleRequest(request).then(response => {
                res.writeHead(response.status, Object.fromEntries(response.headers));
                response.text().then(body => res.end(body));
            });
        });
        server.listen(process.env.PORT || 3000, () => {
            console.log(`HTTP server listening on port ${process.env.PORT || 3000}`);
        });
    });
}
