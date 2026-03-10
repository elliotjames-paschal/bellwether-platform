"""
Tests for pipeline_ingest_feedback.py

Run: pytest packages/pipelines/tests/test_ingest_feedback.py -v
"""

import pytest
import json
import sys
import os
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline_ingest_feedback import (
    normalize_label_type,
    compute_label_id,
    parse_markets_json,
    resolve_market_ids,
    build_ticker_lookup,
    ingest_new_rows,
    parse_timestamp,
    load_human_labels,
    generate_batch_id,
    LABEL_TYPE_MAP,
    DEFAULT_LAST_INGESTED,
)


# ──────────────────────────────────────────────────
# normalize_label_type
# ──────────────────────────────────────────────────


class TestNormalizeLabelType:
    def test_same_event_same_rules(self):
        assert normalize_label_type("same-event:same-rules") == "same_event_same_rules"

    def test_same_event_different_rules(self):
        assert normalize_label_type("same-event:different-rules") == "same_event_different_rules"

    def test_same_event_bare(self):
        """Bare 'same-event' without sub-type defaults to same_rules."""
        assert normalize_label_type("same-event") == "same_event_same_rules"

    def test_not_political(self):
        assert normalize_label_type("not-political") == "not_political"

    def test_wrong_category(self):
        assert normalize_label_type("wrong-category") == "wrong_category"

    def test_different_event(self):
        assert normalize_label_type("different-event") == "different_event"

    def test_other(self):
        assert normalize_label_type("other") == "other"

    def test_case_insensitive(self):
        assert normalize_label_type("SAME-EVENT:SAME-RULES") == "same_event_same_rules"
        assert normalize_label_type("Not-Political") == "not_political"

    def test_whitespace_stripped(self):
        assert normalize_label_type("  same-event  ") == "same_event_same_rules"

    def test_unknown_type_defaults_to_other(self):
        assert normalize_label_type("garbage-type") == "other"
        assert normalize_label_type("") == "other"

    def test_all_map_entries_covered(self):
        """Every entry in the map should return the correct value."""
        for raw, expected in LABEL_TYPE_MAP.items():
            assert normalize_label_type(raw) == expected


# ──────────────────────────────────────────────────
# compute_label_id
# ──────────────────────────────────────────────────


class TestComputeLabelId:
    def test_deterministic(self):
        """Same inputs always produce the same ID."""
        id1 = compute_label_id("2026-01-01T00:00:00Z", ["abc", "def"])
        id2 = compute_label_id("2026-01-01T00:00:00Z", ["abc", "def"])
        assert id1 == id2

    def test_prefix(self):
        """Label IDs start with 'hl_'."""
        lid = compute_label_id("2026-01-01T00:00:00Z", ["abc"])
        assert lid.startswith("hl_")

    def test_length(self):
        """Label ID is hl_ + 12 hex chars = 15 chars total."""
        lid = compute_label_id("2026-01-01T00:00:00Z", ["abc"])
        assert len(lid) == 15

    def test_order_independent(self):
        """Market IDs are sorted, so order doesn't matter."""
        id1 = compute_label_id("2026-01-01T00:00:00Z", ["def", "abc"])
        id2 = compute_label_id("2026-01-01T00:00:00Z", ["abc", "def"])
        assert id1 == id2

    def test_different_timestamps_differ(self):
        id1 = compute_label_id("2026-01-01T00:00:00Z", ["abc"])
        id2 = compute_label_id("2026-01-02T00:00:00Z", ["abc"])
        assert id1 != id2

    def test_different_markets_differ(self):
        id1 = compute_label_id("2026-01-01T00:00:00Z", ["abc"])
        id2 = compute_label_id("2026-01-01T00:00:00Z", ["xyz"])
        assert id1 != id2


# ──────────────────────────────────────────────────
# parse_markets_json
# ──────────────────────────────────────────────────


class TestParseMarketsJson:
    def test_valid_json(self):
        markets = parse_markets_json('[{"key": "BWR-TEST", "label": "Test"}]')
        assert len(markets) == 1
        assert markets[0]["key"] == "BWR-TEST"

    def test_empty_string(self):
        assert parse_markets_json("") == []

    def test_none(self):
        assert parse_markets_json(None) == []

    def test_whitespace(self):
        assert parse_markets_json("   ") == []

    def test_invalid_json(self):
        assert parse_markets_json("not json at all") == []

    def test_json_object_not_array(self):
        """If the JSON is an object instead of array, return empty."""
        assert parse_markets_json('{"key": "value"}') == []

    def test_multiple_markets(self):
        data = json.dumps([
            {"key": "BWR-A", "label": "Market A", "platform": "Kalshi"},
            {"key": "BWR-B", "label": "Market B", "platform": "Polymarket"},
        ])
        markets = parse_markets_json(data)
        assert len(markets) == 2
        assert markets[0]["platform"] == "Kalshi"
        assert markets[1]["platform"] == "Polymarket"


# ──────────────────────────────────────────────────
# build_ticker_lookup + resolve_market_ids
# ──────────────────────────────────────────────────


class TestTickerResolution:
    @pytest.fixture
    def tickers_data(self):
        return {
            "tickers": [
                {"market_id": "12345", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "platform": "Kalshi"},
                {"market_id": "67890", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "platform": "Polymarket"},
                {"market_id": "11111", "ticker": "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028", "platform": "Kalshi"},
            ]
        }

    def test_build_lookup(self, tickers_data):
        lookup = build_ticker_lookup(tickers_data)
        assert "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028" in lookup
        assert len(lookup["BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028"]) == 2
        assert "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028" in lookup

    def test_resolve_known_key(self, tickers_data):
        lookup = build_ticker_lookup(tickers_data)
        resolved = resolve_market_ids(["BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028"], lookup)
        assert resolved == ["11111"]

    def test_resolve_unknown_key_passthrough(self, tickers_data):
        """Unknown keys are passed through as-is (might be raw market IDs)."""
        lookup = build_ticker_lookup(tickers_data)
        resolved = resolve_market_ids(["UNKNOWN-TICKER"], lookup)
        assert resolved == ["UNKNOWN-TICKER"]

    def test_resolve_multiple(self, tickers_data):
        lookup = build_ticker_lookup(tickers_data)
        resolved = resolve_market_ids([
            "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
            "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028",
        ], lookup)
        assert resolved[0] == "12345"  # First market_id for TRUMP ticker
        assert resolved[1] == "11111"

    def test_empty_tickers(self):
        lookup = build_ticker_lookup({"tickers": []})
        assert lookup == {}
        resolved = resolve_market_ids(["BWR-TEST"], lookup)
        assert resolved == ["BWR-TEST"]


# ──────────────────────────────────────────────────
# parse_timestamp
# ──────────────────────────────────────────────────


class TestParseTimestamp:
    def test_iso_format_with_millis(self):
        ts = parse_timestamp("2026-02-12T22:41:28.460Z")
        assert ts.year == 2026
        assert ts.month == 2
        assert ts.day == 12

    def test_iso_format_without_millis(self):
        ts = parse_timestamp("2026-02-12T22:41:28Z")
        assert ts.year == 2026

    def test_google_sheets_format(self):
        ts = parse_timestamp("2/10/2026 15:30:00")
        assert ts.year == 2026
        assert ts.month == 2
        assert ts.day == 10

    def test_standard_datetime(self):
        ts = parse_timestamp("2026-02-10 15:30:00")
        assert ts.year == 2026

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Cannot parse timestamp"):
            parse_timestamp("not a date")

    def test_whitespace_stripped(self):
        ts = parse_timestamp("  2026-02-12T22:41:28Z  ")
        assert ts.year == 2026


# ──────────────────────────────────────────────────
# ingest_new_rows
# ──────────────────────────────────────────────────


class TestIngestNewRows:
    @pytest.fixture
    def empty_existing(self):
        return {
            "schema_version": 1,
            "updated_at": None,
            "last_ingested_timestamp": "2026-02-12T22:41:28.460Z",
            "labels": [],
        }

    @pytest.fixture
    def tickers_data(self):
        return {
            "tickers": [
                {"market_id": "K001", "ticker": "BWR-WARSH-APPOINT-FED_CHAIR-CERTIFIED-ANY-2029"},
                {"market_id": "P001", "ticker": "BWR-WARSH-APPOINT-FED_CHAIR-STD-ANY-2029"},
            ]
        }

    def _make_row(self, timestamp, feedback_type, markets_json, description=""):
        try:
            count = len(json.loads(markets_json)) if markets_json else 0
        except (json.JSONDecodeError, TypeError):
            count = 0
        return {
            "Timestamp": timestamp,
            "Feedback Type": feedback_type,
            "Description": description,
            "Market Count": str(count),
            "Markets (JSON)": markets_json,
        }

    def test_ingests_new_row(self, empty_existing, tickers_data):
        """A row newer than last_ingested_timestamp should be ingested."""
        rows = [self._make_row(
            "2026-03-01T10:00:00.000Z",
            "same-event:same-rules",
            json.dumps([
                {"key": "BWR-WARSH-APPOINT-FED_CHAIR-CERTIFIED-ANY-2029", "label": "Warsh K", "platform": "Kalshi"},
                {"key": "BWR-WARSH-APPOINT-FED_CHAIR-STD-ANY-2029", "label": "Warsh PM", "platform": "Polymarket"},
            ]),
            "Fed chair markets",
        )]

        new_labels, latest_ts = ingest_new_rows(rows, empty_existing, tickers_data)

        assert len(new_labels) == 1
        label = new_labels[0]
        assert label["label_type"] == "same_event_same_rules"
        assert label["status"] == "pending"
        assert label["applied_at"] is None
        assert "K001" in label["market_ids"]
        assert "P001" in label["market_ids"]
        assert label["label_id"].startswith("hl_")
        assert "2026-03-01" in latest_ts

    def test_skips_old_row(self, empty_existing, tickers_data):
        """Rows at or before last_ingested_timestamp should be skipped."""
        rows = [self._make_row(
            "2026-02-10T10:00:00.000Z",
            "same-event",
            json.dumps([{"key": "BWR-TEST", "label": "Test", "platform": "Kalshi"}]),
        )]

        new_labels, latest_ts = ingest_new_rows(rows, empty_existing, tickers_data)
        assert len(new_labels) == 0
        assert latest_ts == "2026-02-12T22:41:28.460Z"

    def test_skips_duplicate(self, empty_existing, tickers_data):
        """If label_id already exists, skip the row."""
        rows = [self._make_row(
            "2026-03-01T10:00:00.000Z",
            "not-political",
            json.dumps([{"key": "BWR-TEST", "label": "Test", "platform": "Kalshi"}]),
        )]

        # Ingest once
        new_labels, _ = ingest_new_rows(rows, empty_existing, tickers_data)
        assert len(new_labels) == 1

        # Add to existing and try again
        empty_existing["labels"].extend(new_labels)
        new_labels2, _ = ingest_new_rows(rows, empty_existing, tickers_data)
        assert len(new_labels2) == 0

    def test_skips_row_without_markets(self, empty_existing, tickers_data):
        """Rows with empty or invalid Markets JSON should be skipped."""
        rows = [
            self._make_row("2026-03-01T10:00:00.000Z", "other", ""),
            self._make_row("2026-03-02T10:00:00.000Z", "other", "not json"),
            self._make_row("2026-03-03T10:00:00.000Z", "other", "[]"),
        ]

        new_labels, _ = ingest_new_rows(rows, empty_existing, tickers_data)
        assert len(new_labels) == 0

    def test_skips_row_without_timestamp(self, empty_existing, tickers_data):
        """Rows with empty timestamp should be skipped."""
        rows = [self._make_row(
            "",
            "not-political",
            json.dumps([{"key": "BWR-TEST", "label": "Test", "platform": "Kalshi"}]),
        )]

        new_labels, _ = ingest_new_rows(rows, empty_existing, tickers_data)
        assert len(new_labels) == 0

    def test_multiple_new_rows(self, empty_existing, tickers_data):
        """Multiple new rows should all be ingested."""
        rows = [
            self._make_row(
                "2026-03-01T10:00:00.000Z",
                "not-political",
                json.dumps([{"key": "MKT1", "label": "M1", "platform": "Kalshi"}]),
            ),
            self._make_row(
                "2026-03-02T10:00:00.000Z",
                "wrong-category",
                json.dumps([{"key": "MKT2", "label": "M2", "platform": "Polymarket"}]),
                "Should be ELECTORAL",
            ),
            self._make_row(
                "2026-03-03T10:00:00.000Z",
                "same-event:different-rules",
                json.dumps([
                    {"key": "MKT3", "label": "M3", "platform": "Kalshi"},
                    {"key": "MKT4", "label": "M4", "platform": "Polymarket"},
                ]),
            ),
        ]

        new_labels, latest_ts = ingest_new_rows(rows, empty_existing, tickers_data)
        assert len(new_labels) == 3
        assert new_labels[0]["label_type"] == "not_political"
        assert new_labels[1]["label_type"] == "wrong_category"
        assert new_labels[1]["description"] == "Should be ELECTORAL"
        assert new_labels[2]["label_type"] == "same_event_different_rules"
        assert "2026-03-03" in latest_ts

    def test_latest_timestamp_tracks_max(self, empty_existing, tickers_data):
        """latest_ts should be the max timestamp of all processed rows."""
        rows = [
            self._make_row(
                "2026-03-05T10:00:00.000Z",
                "other",
                json.dumps([{"key": "A", "label": "A", "platform": "Kalshi"}]),
            ),
            self._make_row(
                "2026-03-03T10:00:00.000Z",
                "other",
                json.dumps([{"key": "B", "label": "B", "platform": "Kalshi"}]),
            ),
        ]

        _, latest_ts = ingest_new_rows(rows, empty_existing, tickers_data)
        assert "2026-03-05" in latest_ts

    def test_platforms_extracted(self, empty_existing, tickers_data):
        """Platforms should be deduplicated from market objects."""
        rows = [self._make_row(
            "2026-03-01T10:00:00.000Z",
            "same-event",
            json.dumps([
                {"key": "A", "label": "A", "platform": "Kalshi"},
                {"key": "B", "label": "B", "platform": "Kalshi"},
                {"key": "C", "label": "C", "platform": "Polymarket"},
            ]),
        )]

        new_labels, _ = ingest_new_rows(rows, empty_existing, tickers_data)
        assert len(new_labels) == 1
        assert set(new_labels[0]["platforms"]) == {"Kalshi", "Polymarket"}

    def test_markets_without_key_skipped(self, empty_existing, tickers_data):
        """Markets without a 'key' field should be filtered out."""
        rows = [self._make_row(
            "2026-03-01T10:00:00.000Z",
            "other",
            json.dumps([{"label": "No key", "platform": "Kalshi"}]),
        )]

        new_labels, _ = ingest_new_rows(rows, empty_existing, tickers_data)
        assert len(new_labels) == 0  # No valid market keys → skip entire row


# ──────────────────────────────────────────────────
# load_human_labels (file I/O)
# ──────────────────────────────────────────────────


class TestLoadHumanLabels:
    def test_creates_default_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pipeline_ingest_feedback.HUMAN_LABELS_FILE", tmp_path / "nonexistent.json")
        data = load_human_labels()
        assert data["schema_version"] == 1
        assert data["last_ingested_timestamp"] == DEFAULT_LAST_INGESTED
        assert data["labels"] == []

    def test_loads_existing_file(self, tmp_path, monkeypatch):
        labels_file = tmp_path / "human_labels.json"
        labels_file.write_text(json.dumps({
            "schema_version": 1,
            "updated_at": "2026-03-01T00:00:00Z",
            "last_ingested_timestamp": "2026-03-01T00:00:00Z",
            "labels": [{"label_id": "hl_test"}],
        }))
        monkeypatch.setattr("pipeline_ingest_feedback.HUMAN_LABELS_FILE", labels_file)
        data = load_human_labels()
        assert len(data["labels"]) == 1
        assert data["labels"][0]["label_id"] == "hl_test"


# ──────────────────────────────────────────────────
# End-to-end with local CSV file
# ──────────────────────────────────────────────────


class TestEndToEndLocalCSV:
    def test_full_ingest_cycle(self, tmp_path, monkeypatch):
        """Simulate a full ingest cycle with a local CSV file."""
        # Create mock CSV
        csv_content = (
            "Timestamp,Feedback Type,Description,Market Count,Markets (JSON)\n"
            '2026-03-01T10:00:00.000Z,same-event:same-rules,Test match,2,'
            '"[{""key"": ""BWR-A"", ""label"": ""A"", ""platform"": ""Kalshi""}, '
            '{""key"": ""BWR-B"", ""label"": ""B"", ""platform"": ""Polymarket""}]"\n'
            '2026-03-02T10:00:00.000Z,not-political,Not political,1,'
            '"[{""key"": ""BWR-C"", ""label"": ""C"", ""platform"": ""Kalshi""}]"\n'
        )
        csv_file = tmp_path / "feedback.csv"
        csv_file.write_text(csv_content)

        # Set up paths
        labels_file = tmp_path / "human_labels.json"
        monkeypatch.setattr("pipeline_ingest_feedback.HUMAN_LABELS_FILE", labels_file)
        monkeypatch.setattr("pipeline_ingest_feedback.TICKERS_FILE", tmp_path / "tickers.json")

        # Create empty tickers file
        (tmp_path / "tickers.json").write_text(json.dumps({"tickers": []}))

        # Load existing (empty)
        from pipeline_ingest_feedback import load_human_labels, load_tickers_data
        existing = load_human_labels()
        tickers = load_tickers_data()

        # Read CSV
        import csv as csv_module
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv_module.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2

        # Ingest
        new_labels, latest_ts = ingest_new_rows(rows, existing, tickers)
        assert len(new_labels) == 2
        assert new_labels[0]["label_type"] == "same_event_same_rules"
        assert new_labels[1]["label_type"] == "not_political"

        # Verify idempotency — second ingest should find nothing new
        existing["labels"].extend(new_labels)
        existing["last_ingested_timestamp"] = latest_ts
        new_labels2, _ = ingest_new_rows(rows, existing, tickers)
        assert len(new_labels2) == 0


# ──────────────────────────────────────────────────
# Batch ID
# ──────────────────────────────────────────────────


class TestBatchId:
    def test_generate_batch_id_format(self):
        """Batch ID should have format batch_YYYYMMDD_HHMMSS."""
        bid = generate_batch_id()
        assert bid.startswith("batch_")
        assert len(bid) == len("batch_YYYYMMDD_HHMMSS")

    def test_batch_id_stamped_on_labels(self):
        """Each ingested label should carry the batch_id."""
        existing = {
            "schema_version": 1,
            "updated_at": None,
            "last_ingested_timestamp": "2026-01-01T00:00:00.000Z",
            "labels": [],
        }
        rows = [{
            "Timestamp": "2026-03-01T10:00:00.000Z",
            "Feedback Type": "not-political",
            "Description": "test",
            "Market Count": "1",
            "Markets (JSON)": json.dumps([{"key": "BWR-X-Y-Z-STD-ANY-2026", "label": "L", "platform": "Kalshi"}]),
        }]
        tickers = {"tickers": []}
        batch_id = "batch_20260310_120000"

        new_labels, _ = ingest_new_rows(rows, existing, tickers, batch_id=batch_id)
        assert len(new_labels) == 1
        assert new_labels[0]["ingested_batch_id"] == "batch_20260310_120000"

    def test_batch_id_none_when_not_provided(self):
        """If no batch_id provided, field should be None."""
        existing = {
            "schema_version": 1,
            "updated_at": None,
            "last_ingested_timestamp": "2026-01-01T00:00:00.000Z",
            "labels": [],
        }
        rows = [{
            "Timestamp": "2026-03-01T10:00:00.000Z",
            "Feedback Type": "not-political",
            "Description": "test",
            "Market Count": "1",
            "Markets (JSON)": json.dumps([{"key": "BWR-X-Y-Z-STD-ANY-2026", "label": "L", "platform": "Kalshi"}]),
        }]
        tickers = {"tickers": []}

        new_labels, _ = ingest_new_rows(rows, existing, tickers)
        assert len(new_labels) == 1
        assert new_labels[0]["ingested_batch_id"] is None
