import type { ToolError } from "../types.js";

const DEFAULT_BASE_URL =
  "https://api.bellwethermetrics.com/api";

function getBaseUrl(): string {
  return process.env.BELLWETHER_API_BASE_URL || DEFAULT_BASE_URL;
}

function getAuthHeaders(): Record<string, string> {
  const key = process.env.BELLWETHER_API_KEY;
  if (key) {
    return { Authorization: `Bearer ${key}` };
  }
  return {};
}

function apiError(message: string): ToolError {
  return { error: "api_unavailable", message };
}

export async function apiGet<T>(
  path: string,
  params?: Record<string, string>
): Promise<T | ToolError> {
  const base = getBaseUrl().replace(/\/$/, "");
  const url = new URL(base + path);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null && v !== "") {
        url.searchParams.set(k, v);
      }
    }
  }

  let response: Response;
  try {
    response = await fetch(url.toString(), {
      headers: {
        Accept: "application/json",
        ...getAuthHeaders(),
      },
    });
  } catch (err) {
    return apiError(
      `Network error: ${err instanceof Error ? err.message : String(err)}`
    );
  }

  if (!response.ok) {
    let body: string;
    try {
      body = await response.text();
    } catch {
      body = "";
    }

    if (response.status === 404) {
      // Let callers handle 404 specifically
      try {
        return JSON.parse(body) as T;
      } catch {
        return apiError(`API returned 404: ${body}`);
      }
    }

    return apiError(
      `API returned ${response.status}: ${body.slice(0, 200)}`
    );
  }

  try {
    return (await response.json()) as T;
  } catch {
    return apiError("Failed to parse API response as JSON");
  }
}

export function isToolError(value: unknown): value is ToolError {
  return (
    typeof value === "object" &&
    value !== null &&
    "error" in value &&
    "message" in value
  );
}
