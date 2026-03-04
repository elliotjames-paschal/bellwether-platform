import { apiGet, isToolError } from "../api/client.js";
import type { LiveMarketResult, EventApiResponse, ToolError } from "../types.js";
import { getGuidance } from "./get_reportability.js";

interface GetMarketInput {
  ticker: string;
}

function tickerToSlug(ticker: string): string {
  return ticker.toLowerCase().replace(/_/g, "-");
}

export async function getMarket(
  input: GetMarketInput
): Promise<LiveMarketResult | ToolError> {
  try {
    if (!input.ticker.startsWith("BWR-")) {
      return {
        error: "invalid_ticker",
        message: `Ticker must start with "BWR-". Got: "${input.ticker}"`,
        ticker: input.ticker,
      };
    }

    const slug = tickerToSlug(input.ticker);
    const data = await apiGet<EventApiResponse | { error: string }>(
      `/metrics/event/${slug}`
    );

    if (isToolError(data)) return data;

    if ("error" in data && !("bellwether_price" in data)) {
      return {
        error: "market_not_found",
        message: `Market not found for ticker "${input.ticker}"`,
        ticker: input.ticker,
      };
    }

    const event = data as EventApiResponse;
    const reportability = event.robustness?.reportability ?? null;
    const costToMove = event.robustness?.cost_to_move_5c ?? null;

    return {
      ticker: event.ticker,
      title: event.title,
      bellwether_price: event.bellwether_price,
      price_label: event.price_label,
      price_tier: event.price_tier as 1 | 2 | 3,
      reportability: reportability as LiveMarketResult["reportability"],
      guidance: getGuidance(reportability, costToMove),
      cost_to_move_5c: costToMove,
      trade_count: event.vwap_details?.trade_count ?? null,
      fetched_at: event.fetched_at,
    };
  } catch (err) {
    return {
      error: "api_unavailable",
      message: `Unexpected error: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}
