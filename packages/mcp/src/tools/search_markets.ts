import { apiGet, isToolError } from "../api/client.js";
import type {
  MarketResult,
  SearchApiResponse,
  ToolError,
  Category,
} from "../types.js";
import { VALID_CATEGORIES } from "../types.js";

interface SearchInput {
  query: string;
  category?: string;
  limit?: number;
}

export async function searchMarkets(
  input: SearchInput
): Promise<{ results: MarketResult[]; total: number; query: string; category: string | null } | ToolError> {
  try {
    if (input.category && !VALID_CATEGORIES.includes(input.category as Category)) {
      return {
        error: "invalid_category",
        message: `Invalid category "${input.category}". Valid values: ${VALID_CATEGORIES.join(", ")}`,
      };
    }

    const params: Record<string, string> = { q: input.query };
    if (input.category) params.category = input.category;
    if (input.limit !== undefined) params.limit = String(input.limit);

    const data = await apiGet<SearchApiResponse>("/markets/search", params);
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
      total: data.total,
      query: data.query,
      category: data.category,
    };
  } catch (err) {
    return {
      error: "api_unavailable",
      message: `Unexpected error: ${err instanceof Error ? err.message : String(err)}`,
    };
  }
}
