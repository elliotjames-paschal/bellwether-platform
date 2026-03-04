import { apiGet, isToolError } from "../api/client.js";
import type {
  MarketResult,
  TopApiResponse,
  ToolError,
  Category,
} from "../types.js";
import { VALID_CATEGORIES } from "../types.js";

interface TopInput {
  category?: string;
  limit?: number;
}

export async function getTopMarkets(
  input: TopInput
): Promise<{ results: MarketResult[]; category: string | null; total_active: number } | ToolError> {
  try {
    if (input.category && !VALID_CATEGORIES.includes(input.category as Category)) {
      return {
        error: "invalid_category",
        message: `Invalid category "${input.category}". Valid values: ${VALID_CATEGORIES.join(", ")}`,
      };
    }

    const params: Record<string, string> = {};
    if (input.category) params.category = input.category;
    if (input.limit !== undefined) params.limit = String(input.limit);

    const data = await apiGet<TopApiResponse>("/markets/top", params);
    if (isToolError(data)) return data;

    const results: MarketResult[] = data.results.map((r) => ({
      ticker: r.ticker,
      title: r.title,
      category: r.category,
      volume_usd: r.volume_usd,
      is_matched: r.is_matched,
      platforms: r.platforms,
    }));

    return {
      results,
      category: data.category,
      total_active: data.total_active,
    };
  } catch (err) {
    return {
      error: "api_unavailable",
      message: `Unexpected error: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}
