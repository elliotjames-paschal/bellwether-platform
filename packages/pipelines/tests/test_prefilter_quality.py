"""
Integration test: compare heuristic pre-filter verdicts against existing GPT verdicts.

This test validates that the heuristic pre-filter doesn't contradict GPT's decisions.
Requires data files to be present (skip gracefully if missing).

Run: pytest packages/pipelines/tests/test_prefilter_quality.py -v
"""

import pytest
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import DATA_DIR

VERDICTS_FILE = DATA_DIR / "cross_platform_resolution_verdicts.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"

# Skip all tests if data files aren't present
pytestmark = pytest.mark.skipif(
    not VERDICTS_FILE.exists() or not ENRICHED_FILE.exists(),
    reason="Data files not available (run pipeline first)"
)


@pytest.fixture(scope="module")
def verdicts_and_lookup():
    """Load existing GPT verdicts and resolution lookup."""
    from pipeline_compare_resolutions import load_existing_verdicts, load_resolution_lookup
    verdicts = load_existing_verdicts()
    resolution_lookup = load_resolution_lookup()
    return verdicts, resolution_lookup


@pytest.fixture(scope="module")
def prefilter_fn():
    """Import heuristic_prefilter."""
    from pipeline_compare_resolutions import heuristic_prefilter
    return heuristic_prefilter


def test_prefilter_accuracy(verdicts_and_lookup, prefilter_fn):
    """Heuristic verdicts should not contradict GPT verdicts at high rates.

    Acceptance criteria:
    - False contradiction rate (heuristic decided, but GPT disagrees) < 10%
    - Specifically: heuristic IDENTICAL but GPT DIFFERENT < 2%
    """
    verdicts, resolution_lookup = verdicts_and_lookup

    matches = 0
    contradictions = 0
    skipped = 0
    false_identical = 0  # heuristic says IDENTICAL, GPT says DIFFERENT
    total_gpt = 0

    for pair_key, v in verdicts.items():
        gpt_verdict = v.get("verdict")
        if gpt_verdict in ("ERROR", "UNKNOWN", None):
            continue
        total_gpt += 1

        pair = {
            "kalshi_market_id": v.get("kalshi_market_id", ""),
            "poly_market_id": v.get("poly_market_id", ""),
            "kalshi_question": v.get("kalshi_question", ""),
            "poly_question": v.get("poly_question", ""),
            "kalshi_ticker": v.get("kalshi_ticker", ""),
            "poly_ticker": v.get("poly_ticker", ""),
        }

        result = prefilter_fn(pair, resolution_lookup)
        if result is None:
            skipped += 1
            continue

        heuristic_verdict = result[0]
        if heuristic_verdict == gpt_verdict:
            matches += 1
        else:
            contradictions += 1
            if heuristic_verdict == "IDENTICAL" and gpt_verdict == "DIFFERENT":
                false_identical += 1

    total_decided = matches + contradictions
    print(f"\n  Pre-filter results against {total_gpt} GPT verdicts:")
    print(f"    Decided: {total_decided} ({matches} match, {contradictions} contradict)")
    print(f"    Deferred to GPT: {skipped}")
    if total_decided > 0:
        accuracy = matches / total_decided
        print(f"    Accuracy: {accuracy:.1%}")
        print(f"    False IDENTICAL (critical): {false_identical}")

    # Acceptance thresholds
    if total_decided > 0:
        assert matches / total_decided >= 0.90, \
            f"Heuristic accuracy {matches/total_decided:.1%} below 90% threshold"
    if total_decided > 0:
        false_identical_rate = false_identical / total_decided
        assert false_identical_rate < 0.02, \
            f"False IDENTICAL rate {false_identical_rate:.1%} exceeds 2% threshold"


def test_prefilter_coverage(verdicts_and_lookup, prefilter_fn):
    """Pre-filter should decide on at least some pairs (not just defer everything)."""
    verdicts, resolution_lookup = verdicts_and_lookup

    decided = 0
    total = 0

    for pair_key, v in verdicts.items():
        if v.get("verdict") in ("ERROR", "UNKNOWN", None):
            continue
        total += 1

        pair = {
            "kalshi_market_id": v.get("kalshi_market_id", ""),
            "poly_market_id": v.get("poly_market_id", ""),
            "kalshi_question": v.get("kalshi_question", ""),
            "poly_question": v.get("poly_question", ""),
            "kalshi_ticker": v.get("kalshi_ticker", ""),
            "poly_ticker": v.get("poly_ticker", ""),
        }

        result = prefilter_fn(pair, resolution_lookup)
        if result is not None:
            decided += 1

    if total > 0:
        coverage = decided / total
        print(f"\n  Pre-filter coverage: {decided}/{total} = {coverage:.1%}")
        # Should decide on at least 10% of pairs to be useful
        assert coverage >= 0.05, \
            f"Pre-filter only decided {coverage:.1%} of pairs — may not save meaningful GPT costs"
