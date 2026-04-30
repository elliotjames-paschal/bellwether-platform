"""
media_pipeline — shared logic for the Bellwether media citation pipeline.

Re-exports the public API from core and schema submodules so callers can do:
    from media_pipeline import extract_market_references, is_duplicate
"""

from media_pipeline.core import (  # noqa: F401
    # Constants
    FUZZY_CANDIDATE_THRESHOLD,
    FUZZY_CANDIDATE_LIMIT,
    LLM_MODEL,
    LLM_MATCH_ENABLED,
    PLATFORM_PROB_PATTERNS,
    GENERIC_PROB_PATTERNS,
    PLATFORM_MENTION,
    POLYMARKET_URL,
    KALSHI_URL,
    POLYMARKET_MARKET_URL,
    STOP_WORDS,
    OUTLET_AUTHORITY,
    WEIGHT_VOLUME,
    WEIGHT_DEPTH,
    WEIGHT_SPREAD,
    WEIGHT_VOLATILITY,
    VOLUME_SATURATION,
    DEPTH_SATURATION,
    TIER1_THRESHOLD,
    TIER2_THRESHOLD,
    VOLATILITY_WINDOWS,
    DEFAULT_FRAGILITY_MISSING,
    PROMO_PATTERNS,
    TOPIC_PATTERNS,
    # Helpers
    _is_missing,
    _to_float,
    _safe_set,
    _safe_str,
    flatten_market,
    extract_keywords,
    build_market_search_text,
    build_market_indices,
    filter_markets_by_platform,
    keyword_prefilter,
    generate_market_url,
    normalize_title,
    # Extraction
    extract_market_references,
    # Matching
    MarketSearchIndex,
    match_by_url,
    get_fuzzy_candidates,
    match_with_llm,
    match_reference_to_market,
    validate_probability_match,
    classify_citation_topic,
    build_topic_clusters,
    # Dedup
    deduplicate_citations,
    # Fragility
    parse_price_timestamp,
    find_price_at_time,
    calculate_price_volatility,
    compute_fragility_score,
    estimate_depth_from_volume,
    assign_tier,
    # Web data helpers
    is_promotional,
    classify_topic,
    compute_outlet_grade,
    domain_to_name,
)

from media_pipeline.schema import (  # noqa: F401
    CITATION_SCHEMA,
    compute_url_hash,
    is_duplicate,
)
