#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { searchMarkets } from "./tools/search_markets.js";
import { getMarket } from "./tools/get_market.js";
import { getTopMarkets } from "./tools/get_top_markets.js";
import { getPlatformSpread } from "./tools/get_platform_spread.js";
import { getReportability } from "./tools/get_reportability.js";
import { VALID_CATEGORIES } from "./types.js";

const server = new McpServer({
  name: "bellwether",
  version: "0.1.0",
});

const categoryEnum = z.enum(VALID_CATEGORIES as unknown as [string, ...string[]]);

// Tool 1: search_markets
server.tool(
  "search_markets",
  "Search active political prediction markets by topic or keyword. Returns matching markets with tickers, titles, and volume. Use this tool first when you need to find a market on a topic — then pass the ticker to get_market for live price data.",
  {
    query: z.string().describe(
      'Topic or keyword to search for. Examples: "senate 2026", "federal reserve", "trump tariffs"'
    ),
    category: categoryEnum
      .optional()
      .describe("Filter by political category"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .optional()
      .describe("Maximum results to return. Default 10, max 50"),
  },
  async ({ query, category, limit }) => {
    const result = await searchMarkets({ query, category, limit });
    return {
      content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
    };
  }
);

// Tool 2: get_market
server.tool(
  "get_market",
  "Get live price, volume-weighted average price (VWAP), and data quality tier for a specific market. Accepts a Bellwether ticker or slug. Always check reportability before citing a price in published content.",
  {
    ticker: z.string().describe(
      "Bellwether ticker or slug. Example: BWR-DEM-CONTROL-HOUSE-CERTIFIED-ANY-2026"
    ),
  },
  async ({ ticker }) => {
    const result = await getMarket({ ticker });
    return {
      content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
    };
  }
);

// Tool 3: get_top_markets
server.tool(
  "get_top_markets",
  "Get the most actively traded prediction markets right now, optionally filtered by political category. Useful for discovering what political questions markets are currently focused on.",
  {
    category: categoryEnum
      .optional()
      .describe("Filter by political category"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .optional()
      .describe("Maximum results to return. Default 10, max 50"),
  },
  async ({ category, limit }) => {
    const result = await getTopMarkets({ category, limit });
    return {
      content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
    };
  }
);

// Tool 4: get_platform_spread
server.tool(
  "get_platform_spread",
  "Compare prices between Kalshi and Polymarket for a matched market. Returns the spread in cents and which platform is pricing higher. Only works for cross-platform matched markets (is_matched: true).",
  {
    ticker: z.string().describe(
      "Bellwether ticker for a matched market"
    ),
  },
  async ({ ticker }) => {
    const result = await getPlatformSpread({ ticker });
    return {
      content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
    };
  }
);

// Tool 5: get_reportability
server.tool(
  "get_reportability",
  "Check whether a prediction market is safe to cite in published content. Returns a reportability tier (Reportable, Caution, or Fragile) and plain-English editorial guidance. Always call this before citing a market price in an article or report.",
  {
    ticker: z.string().describe("Bellwether ticker or slug"),
  },
  async ({ ticker }) => {
    const result = await getReportability({ ticker });
    return {
      content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
    };
  }
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
