"""
Schema utilities for the media pipeline.

Provides URL hashing and deduplication helpers used across pipeline stages.
"""

import hashlib


# Minimal schema documentation for internal citation records.
# Not enforced at runtime; serves as a reference for expected fields.
CITATION_SCHEMA = {
    "required": ["url", "title", "source_type", "id"],
    "optional": [
        "domain", "published_date", "sentence", "context",
        "discovery_source", "discovered_at", "url_hash",
        "market_references", "match_status",
    ],
}


def compute_url_hash(url):
    """Compute a SHA-256 hash of a URL string."""
    return hashlib.sha256(url.encode()).hexdigest()


def is_duplicate(citation, seen):
    """Check if a citation URL has already been seen.

    Computes url_hash on the fly if the field is missing (backward compat).
    Adds the hash to `seen` if new.  Returns True if duplicate.
    """
    url_hash = citation.get("url_hash")
    if not url_hash:
        url = citation.get("url", "")
        if not url:
            return False
        url_hash = compute_url_hash(url)
    if url_hash in seen:
        return True
    seen.add(url_hash)
    return False
