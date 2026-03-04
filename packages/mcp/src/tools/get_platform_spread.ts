import { apiGet, isToolError } from "../api/client.js";
import type {
  PlatformSpreadResult,
  EventApiResponse,
  ToolError,
} from "../types.js";

interface SpreadInput {
  ticker: string;
}

function tickerToSlug(ticker: string): string {
  return ticker.toLowerCase().replace(/_/g, "-");
}

export async function getPlatformSpread(
  input: SpreadInput
): Promise<PlatformSpreadResult | ToolError> {
  try {
    if (!input.ticker.startsWith("BWR-")) {
      return {
        error: "invalid_ticker",
        message: `Ticker must start with "BWR-". Got: "${input.ticker}"`,
        ticker: input.ticker,
      };
    }

    const slug = tickerToSlug(input.ticker);

    // Step 1: Get event data to check if matched and get platform IDs
    const eventData = await apiGet<EventApiResponse | { error: string }>(
      `/metrics/event/${slug}`
    );

    if (isToolError(eventData)) return eventData;

    if ("error" in eventData && !("bellwether_price" in eventData)) {
      return {
        error: "market_not_found",
        message: `Market not found for ticker "${input.ticker}"`,
        ticker: input.ticker,
      };
    }

    const event = eventData as EventApiResponse;

    // Step 2: Check if matched (has both platforms)
    const pmToken = event.platform_prices?.polymarket;
    const kTicker = event.platform_prices?.kalshi;

    if (!pmToken || !kTicker) {
      return {
        error: "not_matched",
        message: `Market "${input.ticker}" is only on one platform. get_platform_spread requires a cross-platform matched market.`,
        ticker: input.ticker,
      };
    }

    // Step 3: Get individual platform prices
    const [pmData, kData] = await Promise.all([
      apiGet<{ bellwether_price: number | null; fetched_at: string } | { error: string }>(
        `/metrics/polymarket/${pmToken}`
      ),
      apiGet<{ bellwether_price: number | null; fetched_at: string } | { error: string }>(
        `/metrics/kalshi/${kTicker}`
      ),
    ]);

    const pmPrice =
      !isToolError(pmData) && "bellwether_price" in pmData
        ? pmData.bellwether_price
        : null;
    const kPrice =
      !isToolError(kData) && "bellwether_price" in kData
        ? kData.bellwether_price
        : null;

    // Step 4: Compute spread
    let spreadCents: number | null = null;
    let spreadDirection: PlatformSpreadResult["spread_direction"] =
      "insufficient_data";

    if (pmPrice !== null && kPrice !== null) {
      spreadCents = Math.round(Math.abs(pmPrice - kPrice) * 100);
      if (spreadCents === 0) {
        spreadDirection = "equal";
      } else if (kPrice > pmPrice) {
        spreadDirection = "kalshi_higher";
      } else {
        spreadDirection = "polymarket_higher";
      }
    }

    return {
      ticker: event.ticker,
      kalshi_price: kPrice,
      polymarket_price: pmPrice,
      spread_cents: spreadCents,
      spread_direction: spreadDirection,
      reportability: event.robustness?.reportability ?? "fragile",
      fetched_at: event.fetched_at,
    };
  } catch (err) {
    return {
      error: "api_unavailable",
      message: `Unexpected error: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}
