"""
Tests for pipeline error handling — ensures individual step failures
don't crash the entire pipeline.

Run: pytest packages/pipelines/tests/test_pipeline_errors.py -v
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class TestClassifyNewTags:
    """
    pipeline_refresh_political_tags.py: future.result() re-raises worker
    exceptions, crashing the whole classification step if one GPT batch fails.
    """

    def test_one_failed_batch_doesnt_crash(self):
        """If one batch fails (API error), other batches should still succeed."""
        results = []
        errors = []

        def good_batch(idx, batch):
            return [{"slug": "politics"}], [{"slug": "sports"}]

        def bad_batch(idx, batch):
            raise Exception("OpenAI API timeout")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(good_batch, 0, []): 0,
                executor.submit(bad_batch, 1, []): 1,
                executor.submit(good_batch, 2, []): 2,
            }

            for future in as_completed(futures):
                try:
                    political, rejected = future.result()
                    results.extend(political)
                except Exception as e:
                    errors.append(str(e))

        assert len(results) == 2  # two good batches succeeded
        assert len(errors) == 1  # one bad batch caught
        assert "timeout" in errors[0]

    def test_all_batches_fail_returns_empty(self):
        """If every batch fails, we get empty results, not a crash."""
        results = []
        errors = []

        def bad_batch(idx, batch):
            raise Exception("rate limited")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(bad_batch, i, []): i
                for i in range(3)
            }

            for future in as_completed(futures):
                try:
                    political, rejected = future.result()
                    results.extend(political)
                except Exception as e:
                    errors.append(str(e))

        assert len(results) == 0
        assert len(errors) == 3

    def test_classify_tags_batch_malformed_response(self):
        """GPT returns garbage — should not crash, tags default to rejected."""
        from pipeline_refresh_political_tags import classify_tags_batch

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="this is not valid output"))]
        )

        tags = [
            {"id": "1", "label": "US Politics", "slug": "us-politics"},
            {"id": "2", "label": "NBA", "slug": "nba"},
        ]

        political, rejected = classify_tags_batch(mock_client, tags)
        # Unparseable responses default to rejected
        assert len(political) == 0
        assert len(rejected) == 2

    def test_classify_tags_batch_valid_response(self):
        """GPT returns proper format — tags sorted correctly."""
        from pipeline_refresh_political_tags import classify_tags_batch

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content="Tag 1: YES\nTag 2: NO"))]
        )

        tags = [
            {"id": "1", "label": "US Politics", "slug": "us-politics"},
            {"id": "2", "label": "NBA", "slug": "nba"},
        ]

        political, rejected = classify_tags_batch(mock_client, tags)
        assert len(political) == 1
        assert len(rejected) == 1
        assert political[0]["slug"] == "us-politics"

    def test_classify_tags_batch_empty_response(self):
        """GPT returns empty string."""
        from pipeline_refresh_political_tags import classify_tags_batch

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(content=""))]
        )

        tags = [{"id": "1", "label": "Test", "slug": "test"}]
        political, rejected = classify_tags_batch(mock_client, tags)
        assert len(rejected) == 1  # defaults to rejected
