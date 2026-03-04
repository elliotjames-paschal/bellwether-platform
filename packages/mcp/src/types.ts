// Response contracts for MCP tool outputs

export interface MarketResult {
  ticker: string;
  title: string;
  category: string;
  volume_usd: number;
  is_matched: boolean;
  platforms: string[];
}

export interface LiveMarketResult {
  ticker: string;
  title: string;
  bellwether_price: number | null;
  price_label: string;
  price_tier: 1 | 2 | 3;
  reportability: "reportable" | "caution" | "fragile" | null;
  guidance: string;
  cost_to_move_5c: number | null;
  trade_count: number | null;
  fetched_at: string;
}

export interface PlatformSpreadResult {
  ticker: string;
  kalshi_price: number | null;
  polymarket_price: number | null;
  spread_cents: number | null;
  spread_direction:
    | "kalshi_higher"
    | "polymarket_higher"
    | "equal"
    | "insufficient_data";
  reportability: string;
  fetched_at: string;
}

export interface ReportabilityResult {
  ticker: string;
  reportability: "reportable" | "caution" | "fragile";
  cost_to_move_5c: number | null;
  guidance: string;
  price_tier: 1 | 2 | 3;
  fetched_at: string;
}

export interface ToolError {
  error: string;
  message: string;
  ticker?: string;
}

// Raw API response types

export interface SearchApiResponse {
  results: Array<{
    slug: string;
    ticker: string;
    title: string;
    category: string;
    volume_usd: number;
    is_matched: boolean;
    platforms: string[];
  }>;
  total: number;
  query: string;
  category: string | null;
}

export interface TopApiResponse {
  results: Array<{
    slug: string;
    ticker: string;
    title: string;
    category: string;
    volume_usd: number;
    is_matched: boolean;
    platforms: string[];
  }>;
  category: string | null;
  total_active: number;
}

export interface EventApiResponse {
  slug: string;
  title: string;
  category: string;
  country: string;
  platform: string;
  ticker: string;
  bellwether_price: number | null;
  price_tier: number;
  price_label: string;
  price_source: string;
  platform_prices: {
    polymarket: string | null;
    kalshi: string | null;
  };
  robustness: {
    cost_to_move_5c: number | null;
    reportability: string;
    raw_reportability: string;
    weakest_platform: string | null;
  };
  vwap_details: {
    window_hours: number | null;
    trade_count: number;
    total_volume: number;
  };
  orderbook_midpoint: number | null;
  fetched_at: string;
}

export interface CombinedApiResponse {
  ticker: string | null;
  bellwether_price: number | null;
  price_tier: number;
  price_label: string;
  price_source: string;
  robustness: {
    cost_to_move_5c: number | null;
    reportability: string;
    raw_reportability: string;
    weakest_platform: string;
  };
  vwap_details: {
    window_hours: number | null;
    trade_count: number;
    total_volume: number;
  };
  orderbook_midpoint: number | null;
  fetched_at: string;
}

export const VALID_CATEGORIES = [
  "ELECTORAL",
  "MONETARY_POLICY",
  "INTERNATIONAL",
  "POLITICAL_SPEECH",
  "MILITARY_SECURITY",
  "APPOINTMENTS",
  "TIMING_EVENTS",
  "JUDICIAL",
  "PARTY_POLITICS",
  "GOVERNMENT_OPERATIONS",
  "REGULATORY",
  "LEGISLATIVE",
  "POLLING_APPROVAL",
  "STATE_LOCAL",
  "CRISIS_EMERGENCY",
] as const;

export type Category = (typeof VALID_CATEGORIES)[number];
