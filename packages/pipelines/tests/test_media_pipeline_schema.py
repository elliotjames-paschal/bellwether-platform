"""
Tests for media_pipeline.schema — URL hashing and deduplication.

Run: pytest packages/pipelines/tests/test_media_pipeline_schema.py -v
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from media_pipeline.schema import compute_url_hash, is_duplicate, CITATION_SCHEMA


class TestComputeUrlHash:
    def test_deterministic(self):
        url = "https://example.com/article/123"
        assert compute_url_hash(url) == compute_url_hash(url)

    def test_different_urls_different_hashes(self):
        h1 = compute_url_hash("https://example.com/a")
        h2 = compute_url_hash("https://example.com/b")
        assert h1 != h2

    def test_empty_url(self):
        h = compute_url_hash("")
        assert isinstance(h, str) and len(h) == 64

    def test_sha256_length(self):
        h = compute_url_hash("https://polymarket.com/event/some-slug")
        assert len(h) == 64  # SHA-256 hex digest


class TestIsDuplicate:
    def test_first_seen_returns_false(self):
        seen = set()
        c = {"url": "https://example.com/1", "title": "Test"}
        assert is_duplicate(c, seen) is False
        assert len(seen) == 1

    def test_second_seen_returns_true(self):
        seen = set()
        c = {"url": "https://example.com/1", "title": "Test"}
        is_duplicate(c, seen)
        assert is_duplicate(c, seen) is True

    def test_different_urls_not_duplicate(self):
        seen = set()
        c1 = {"url": "https://example.com/1"}
        c2 = {"url": "https://example.com/2"}
        is_duplicate(c1, seen)
        assert is_duplicate(c2, seen) is False
        assert len(seen) == 2

    def test_uses_url_hash_field_if_present(self):
        seen = set()
        h = compute_url_hash("https://example.com/1")
        c = {"url": "https://example.com/1", "url_hash": h}
        assert is_duplicate(c, seen) is False
        assert is_duplicate(c, seen) is True

    def test_no_url_returns_false(self):
        seen = set()
        c = {"title": "No URL"}
        assert is_duplicate(c, seen) is False

    def test_empty_url_returns_false(self):
        seen = set()
        c = {"url": ""}
        assert is_duplicate(c, seen) is False

    def test_backward_compat_without_url_hash(self):
        """Citations without url_hash field should still dedup correctly."""
        seen = set()
        c1 = {"url": "https://reuters.com/article/abc"}
        c2 = {"url": "https://reuters.com/article/abc"}
        assert is_duplicate(c1, seen) is False
        assert is_duplicate(c2, seen) is True

    def test_mixed_with_and_without_url_hash(self):
        """A citation with url_hash should match one computed on the fly."""
        seen = set()
        url = "https://example.com/test"
        c1 = {"url": url}  # no url_hash
        c2 = {"url": url, "url_hash": compute_url_hash(url)}  # has url_hash
        assert is_duplicate(c1, seen) is False
        assert is_duplicate(c2, seen) is True


class TestCitationSchema:
    def test_schema_has_required_fields(self):
        assert "url" in CITATION_SCHEMA["required"]
        assert "id" in CITATION_SCHEMA["required"]

    def test_schema_has_optional_fields(self):
        assert "discovery_source" in CITATION_SCHEMA["optional"]
        assert "discovered_at" in CITATION_SCHEMA["optional"]
        assert "url_hash" in CITATION_SCHEMA["optional"]
