#!/usr/bin/env python3
"""
PredictionHunt API client for cross-platform market matching validation.

API docs: https://www.predictionhunt.com/api/docs

Usage:
    # As module
    from predictionhunt_client import PredictionHuntClient
    client = PredictionHuntClient()
    result = client.query_by_kalshi_ticker("PRES-2028-R")

    # Test connectivity
    python predictionhunt_client.py --test
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import DATA_DIR, get_predictionhunt_api_key

BASE_URL = "https://www.predictionhunt.com/api/v1"
USAGE_FILE = DATA_DIR / "predictionhunt_usage.json"
MONTHLY_LIMIT = 1000


class BudgetExhaustedError(Exception):
    pass


def kalshi_market_id_to_event_ticker(market_id):
    """Derive Kalshi event ticker from a market ID.

    PredictionHunt expects event-level tickers (e.g., 'KXPRESPERSON-28')
    not market-level IDs (e.g., 'KXPRESPERSON-28-JVAN').

    Strategy: try progressively shorter prefixes until we find one that
    looks like an event ticker (ends with a year/number segment).
    """
    parts = market_id.split("-")
    if len(parts) <= 2:
        return market_id

    # Common pattern: EVENT-YEAR-CANDIDATE → return EVENT-YEAR
    # e.g., KXPRESPERSON-28-JVAN → KXPRESPERSON-28
    # e.g., KXHONDURASPRESIDENTMOV-25NOV30-7 → KXHONDURASPRESIDENTMOV-25NOV30
    # Try removing last segment first
    return "-".join(parts[:-1])


class PredictionHuntClient:
    """Lightweight client for PredictionHunt matching-markets API."""

    def __init__(self, api_key=None, delay=1.0):
        self.api_key = api_key or get_predictionhunt_api_key()
        self.delay = delay
        self._last_request_time = 0
        self._session = requests.Session()
        self._session.headers["X-API-Key"] = self.api_key

    def _rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    def _load_usage(self):
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        if USAGE_FILE.exists():
            with open(USAGE_FILE) as f:
                usage = json.load(f)
            if usage.get("current_month") != current_month:
                usage = {"monthly_limit": MONTHLY_LIMIT, "current_month": current_month,
                         "requests_used": 0, "requests_log": []}
        else:
            usage = {"monthly_limit": MONTHLY_LIMIT, "current_month": current_month,
                     "requests_used": 0, "requests_log": []}
        return usage

    def _save_usage(self, usage):
        from config import atomic_write_json
        atomic_write_json(USAGE_FILE, usage, indent=2, ensure_ascii=False)

    def _increment_usage(self, pipeline="unknown"):
        usage = self._load_usage()
        usage["requests_used"] += 1
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for entry in usage["requests_log"]:
            if entry["date"] == today and entry["pipeline"] == pipeline:
                entry["count"] += 1
                break
        else:
            usage["requests_log"].append({"date": today, "count": 1, "pipeline": pipeline})
        self._save_usage(usage)

    def check_budget(self):
        """Return (remaining, used, limit). Raises BudgetExhaustedError if at limit."""
        usage = self._load_usage()
        remaining = usage["monthly_limit"] - usage["requests_used"]
        if remaining <= 0:
            raise BudgetExhaustedError(
                f"PredictionHunt monthly budget exhausted ({usage['requests_used']}/{usage['monthly_limit']})")
        return remaining, usage["requests_used"], usage["monthly_limit"]

    def query_by_kalshi_ticker(self, kalshi_ticker, pipeline="unknown"):
        """Query PredictionHunt for cross-platform matches by Kalshi ticker.

        Automatically derives the event-level ticker from a market-level ID
        since PH expects event tickers (e.g., 'KXPRESPERSON-28' not 'KXPRESPERSON-28-JVAN').

        Returns:
            dict with keys: success, count, events, raw_response, rate_limit
            On error: dict with success=False and error key
        """
        event_ticker = kalshi_market_id_to_event_ticker(kalshi_ticker)

        self.check_budget()
        self._rate_limit()

        try:
            resp = self._session.get(
                f"{BASE_URL}/matching-markets",
                params={"kalshi_tickers": event_ticker},
                timeout=15,
            )

            rate_limit = {
                "remaining_second": resp.headers.get("X-RateLimit-Remaining-Second"),
                "remaining_month": resp.headers.get("X-RateLimit-Remaining-Month"),
            }

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "60")
                return {"success": False, "error": "rate_limited", "retry_after": int(retry_after),
                        "rate_limit": rate_limit}

            if resp.status_code != 200:
                return {"success": False, "error": f"http_{resp.status_code}",
                        "body": resp.text[:500], "rate_limit": rate_limit}

            data = resp.json()
            if data.get("success"):
                self._increment_usage(pipeline)
            return {**data, "rate_limit": rate_limit}

        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def query_by_polymarket_slug(self, slug, pipeline="unknown"):
        """Query PredictionHunt for cross-platform matches by Polymarket slug."""
        self.check_budget()
        self._rate_limit()

        try:
            resp = self._session.get(
                f"{BASE_URL}/matching-markets",
                params={"polymarket_slugs": slug},
                timeout=15,
            )

            rate_limit = {
                "remaining_second": resp.headers.get("X-RateLimit-Remaining-Second"),
                "remaining_month": resp.headers.get("X-RateLimit-Remaining-Month"),
            }

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "60")
                return {"success": False, "error": "rate_limited", "retry_after": int(retry_after),
                        "rate_limit": rate_limit}

            if resp.status_code != 200:
                return {"success": False, "error": f"http_{resp.status_code}",
                        "body": resp.text[:500], "rate_limit": rate_limit}

            data = resp.json()
            if data.get("success"):
                self._increment_usage(pipeline)
            return {**data, "rate_limit": rate_limit}

        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def extract_platform_ids(self, response):
        """Extract platform-specific market IDs from a PredictionHunt response.

        Returns:
            dict: {
                "kalshi_ids": [{"id": ..., "source_url": ..., "group_title": ...}, ...],
                "polymarket_ids": [{"id": ..., "source_url": ..., "group_title": ...}, ...],
            }
        """
        result = {"kalshi_ids": [], "polymarket_ids": []}
        if not response.get("success") or not response.get("events"):
            return result

        for event in response["events"]:
            for group in event.get("groups", []):
                group_title = group.get("title", "")
                for market in group.get("markets", []):
                    source = market.get("source", "").lower()
                    entry = {
                        "id": market.get("id", ""),
                        "source_url": market.get("source_url", ""),
                        "group_title": group_title,
                        "event_title": event.get("title", ""),
                    }
                    if "kalshi" in source:
                        result["kalshi_ids"].append(entry)
                    elif "polymarket" in source:
                        result["polymarket_ids"].append(entry)

        return result

    def find_group_for_kalshi_market(self, response, kalshi_market_id):
        """Find the specific group containing a Kalshi market ID.

        PH returns all groups for an event. This finds the one group
        that contains the specific Kalshi market we're interested in,
        and returns Polymarket market info from that same group.

        Returns:
            list of dicts: [{"id": ..., "source_url": ...}, ...] or empty list
        """
        if not response.get("success") or not response.get("events"):
            return []

        k_id_lower = kalshi_market_id.lower()
        for event in response["events"]:
            for group in event.get("groups", []):
                kalshi_ids_in_group = []
                poly_markets_in_group = []
                for market in group.get("markets", []):
                    source = market.get("source", "").lower()
                    if "kalshi" in source:
                        kalshi_ids_in_group.append(market.get("id", ""))
                    elif "polymarket" in source:
                        poly_markets_in_group.append({
                            "id": market.get("id", ""),
                            "source_url": market.get("source_url", ""),
                        })

                if any(kid.lower() == k_id_lower for kid in kalshi_ids_in_group):
                    return poly_markets_in_group

        return []

    def get_usage_summary(self):
        """Return current usage stats."""
        usage = self._load_usage()
        return {
            "month": usage["current_month"],
            "used": usage["requests_used"],
            "limit": usage["monthly_limit"],
            "remaining": usage["monthly_limit"] - usage["requests_used"],
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PredictionHunt API client")
    parser.add_argument("--test", action="store_true", help="Test API connectivity")
    parser.add_argument("--kalshi", type=str, help="Query by Kalshi ticker")
    parser.add_argument("--polymarket", type=str, help="Query by Polymarket slug")
    parser.add_argument("--usage", action="store_true", help="Show usage stats")
    args = parser.parse_args()

    client = PredictionHuntClient()

    if args.usage:
        print(json.dumps(client.get_usage_summary(), indent=2))

    elif args.test:
        print("Testing PredictionHunt API connectivity...")
        test_ticker = "KXPRESPERSON-28-JVAN"
        print(f"  Querying with market ID: {test_ticker}")
        print(f"  Derived event ticker: {kalshi_market_id_to_event_ticker(test_ticker)}")
        result = client.query_by_kalshi_ticker(test_ticker, pipeline="test")
        if result.get("success"):
            ids = client.extract_platform_ids(result)
            print(f"  Success! Found {result.get('count')} event(s)")
            print(f"  Kalshi markets: {len(ids['kalshi_ids'])}")
            print(f"  Polymarket markets: {len(ids['polymarket_ids'])}")
            # Find specific group match
            group_pm = client.find_group_for_kalshi_market(result, test_ticker)
            print(f"  PM matches in same group as {test_ticker}: {group_pm}")
        else:
            print(f"  Failed: {result.get('error', 'unknown')}")
            print(json.dumps(result, indent=2, default=str))
        print(f"\nUsage: {json.dumps(client.get_usage_summary(), indent=2)}")

    elif args.kalshi:
        result = client.query_by_kalshi_ticker(args.kalshi, pipeline="manual")
        print(json.dumps(result, indent=2, default=str))

    elif args.polymarket:
        result = client.query_by_polymarket_slug(args.polymarket, pipeline="manual")
        print(json.dumps(result, indent=2, default=str))

    else:
        parser.print_help()
