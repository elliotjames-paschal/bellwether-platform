import { apiGet, isToolError } from "../api/client.js";
import type { ReportabilityResult, EventApiResponse, ToolError } from "../types.js";

interface ReportabilityInput {
  ticker: string;
}

function tickerToSlug(ticker: string): string {
  return ticker.toLowerCase().replace(/_/g, "-");
}

export function getGuidance(
  reportability: string | null,
  costToMove: number | null
): string {
  switch (reportability) {
    case "reportable":
      return `Safe to cite. Price reflects broad market consensus with high liquidity. Cost to move price 5 cents: $${costToMove}.`;
    case "caution":
      return `Cite with context. Liquidity is moderate — note the market's liquidity level alongside any price you report. Cost to move price 5 cents: $${costToMove}.`;
    case "fragile":
      return `Do not cite as market consensus. This market has low liquidity and could be manipulated with a small amount of capital. Cost to move price 5 cents: $${costToMove}.`;
    default:
      return "Reportability cannot be assessed — insufficient trade data.";
  }
}

export async function getReportability(
  input: ReportabilityInput
): Promise<ReportabilityResult | ToolError> {
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
    const reportability = (event.robustness?.reportability ?? "fragile") as ReportabilityResult["reportability"];
    const costToMove = event.robustness?.cost_to_move_5c ?? null;

    return {
      ticker: event.ticker,
      reportability,
      cost_to_move_5c: costToMove,
      guidance: getGuidance(reportability, costToMove),
      price_tier: event.price_tier as 1 | 2 | 3,
      fetched_at: event.fetched_at,
    };
  } catch (err) {
    return {
      error: "api_unavailable",
      message: `Unexpected error: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}
