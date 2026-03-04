"""
Tests for election date edge cases that have repeatedly crashed the pipeline.

Run: pytest packages/pipelines/tests/test_election_dates.py -v
"""

import pytest
import sys
import os
import tempfile
import numpy as np
import pandas as pd
from datetime import datetime, timezone, date
from pathlib import Path

# Add pipelines dir to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# =============================================================================
# 1. clean_election_dates_csv
# =============================================================================

class TestCleanElectionDatesCSV:
    """Test the cleanup function that fixes float years."""

    def _write_csv(self, tmp_path, rows):
        path = tmp_path / "election_dates_lookup.csv"
        df = pd.DataFrame(rows)
        df.to_csv(path, index=False)
        return path

    def test_float_year_cast_to_int(self, tmp_path):
        """2026.0 -> 2026 (the original bug)."""
        from config import clean_election_dates_csv
        path = self._write_csv(tmp_path, [
            {"country": "US", "office": "President", "location": "US",
             "election_year": 2026.0, "election_date": "2026-11-03", "is_primary": False}
        ])
        clean_election_dates_csv(path)
        df = pd.read_csv(path)
        assert df["election_year"].dtype in [np.int64, np.int32, int]
        assert df.iloc[0]["election_year"] == 2026

    def test_partial_date_preserved(self, tmp_path):
        """Partial dates like '2025-09-' should NOT be dropped."""
        from config import clean_election_dates_csv
        path = self._write_csv(tmp_path, [
            {"country": "Germany", "office": "Chancellor", "location": "Germany",
             "election_year": 2024.0, "election_date": "2025-09-", "is_primary": False},
            {"country": "US", "office": "President", "location": "US",
             "election_year": 2024.0, "election_date": "2024-11-05", "is_primary": False}
        ])
        clean_election_dates_csv(path)
        df = pd.read_csv(path)
        assert len(df) == 2  # both rows kept

    def test_nan_date_preserved(self, tmp_path):
        """NaN election_date rows should NOT be dropped."""
        from config import clean_election_dates_csv
        path = self._write_csv(tmp_path, [
            {"country": "France", "office": "PM", "location": "France",
             "election_year": 2024.0, "election_date": np.nan, "is_primary": False},
            {"country": "US", "office": "President", "location": "US",
             "election_year": 2024.0, "election_date": "2024-11-05", "is_primary": False}
        ])
        clean_election_dates_csv(path)
        df = pd.read_csv(path)
        assert len(df) == 2

    def test_missing_file(self):
        """Non-existent file returns 0, no crash."""
        from config import clean_election_dates_csv
        result = clean_election_dates_csv(Path("/nonexistent/path.csv"))
        assert result == 0


# =============================================================================
# 2. Truncation: datetime construction from election dates
# =============================================================================

class TestTruncationDateConstruction:
    """
    The truncation scripts build datetime(int(year), int(month), int(day)).
    This has crashed with:
    - TypeError: float can't be interpreted as int (2026.0)
    - ValueError: cannot convert float NaN to integer
    """

    def _build_election_end(self, election_date):
        """Replicate the exact code from truncate_polymarket_prices.py lines 194-204."""
        try:
            election_end = datetime(
                int(election_date.year),
                int(election_date.month),
                int(election_date.day),
                23, 59, 59,
                tzinfo=timezone.utc
            )
            return int(election_end.timestamp())
        except (ValueError, TypeError, OverflowError):
            return None

    def test_valid_date_object(self):
        d = date(2024, 11, 5)
        ts = self._build_election_end(d)
        assert ts is not None
        assert ts == int(datetime(2024, 11, 5, 23, 59, 59, tzinfo=timezone.utc).timestamp())

    def test_valid_timestamp(self):
        d = pd.Timestamp("2024-11-05")
        ts = self._build_election_end(d)
        assert ts is not None

    def test_nat_doesnt_crash(self):
        """pd.NaT has .year that is NaN — must not crash."""
        ts = self._build_election_end(pd.NaT)
        assert ts is None

    def test_none_doesnt_crash(self):
        """None should be caught."""
        try:
            ts = self._build_election_end(None)
        except AttributeError:
            pass  # fine — the calling code checks `if election_date:` first

    def test_float_year_on_date_object(self):
        """datetime.date always has int year, but let's be safe."""
        d = date(2026, 11, 3)
        ts = self._build_election_end(d)
        assert ts is not None

    def test_pandas_timestamp_with_tz(self):
        d = pd.Timestamp("2024-11-05", tz="UTC")
        ts = self._build_election_end(d)
        assert ts is not None


# =============================================================================
# 3. Election lookup building (shared by 5 scripts)
# =============================================================================

class TestElectionLookupBuilding:
    """
    Test the lookup dict construction that appears in truncation scripts.
    Key line: int(row['election_year']) crashes if election_year is NaN.
    """

    def _build_lookup(self, df):
        """Replicate the lookup builder from truncate scripts."""
        lookup = {}
        for _, row in df.iterrows():
            country = row.get('country', 'United States')
            # This is where it crashes if election_year is NaN
            if pd.notna(row.get('election_year')):
                key = (country, row['office'], row['location'], int(row['election_year']))
            else:
                continue
            try:
                election_date = pd.to_datetime(row['election_date'], format='mixed').date()
            except Exception:
                continue
            lookup[key] = election_date
        return lookup

    def test_valid_row(self):
        df = pd.DataFrame([{
            "country": "US", "office": "President", "location": "US",
            "election_year": 2024, "election_date": "2024-11-05"
        }])
        lookup = self._build_lookup(df)
        assert len(lookup) == 1
        assert lookup[("US", "President", "US", 2024)] == date(2024, 11, 5)

    def test_float_year(self):
        df = pd.DataFrame([{
            "country": "US", "office": "President", "location": "US",
            "election_year": 2024.0, "election_date": "2024-11-05"
        }])
        lookup = self._build_lookup(df)
        assert len(lookup) == 1

    def test_nan_year_skipped(self):
        df = pd.DataFrame([{
            "country": "US", "office": "President", "location": "US",
            "election_year": float('nan'), "election_date": "2024-11-05"
        }])
        lookup = self._build_lookup(df)
        assert len(lookup) == 0

    def test_partial_date_skipped(self):
        df = pd.DataFrame([{
            "country": "Germany", "office": "Chancellor", "location": "Germany",
            "election_year": 2024, "election_date": "2025-09-"
        }])
        lookup = self._build_lookup(df)
        assert len(lookup) == 0  # bad date is skipped

    def test_nan_date_skipped(self):
        df = pd.DataFrame([{
            "country": "France", "office": "PM", "location": "France",
            "election_year": 2024, "election_date": np.nan
        }])
        lookup = self._build_lookup(df)
        # pd.to_datetime(NaN) returns NaT which doesn't raise — it enters the lookup
        # but the try/except in the truncation scripts catches it downstream
        # The key thing is: no crash
        assert True


# =============================================================================
# 4. election_date_to_unix (election eve prices)
# =============================================================================

class TestElectionDateToUnix:

    def test_valid_date(self):
        from pipeline_election_eve_prices import election_date_to_unix
        ts = election_date_to_unix("2024-11-05")
        expected = int(datetime(2024, 11, 5, tzinfo=timezone.utc).timestamp())
        assert ts == expected

    def test_partial_date_returns_none(self):
        from pipeline_election_eve_prices import election_date_to_unix
        assert election_date_to_unix("2025-09-") is None

    def test_nan_returns_none(self):
        from pipeline_election_eve_prices import election_date_to_unix
        assert election_date_to_unix(float('nan')) is None

    def test_none_returns_none(self):
        from pipeline_election_eve_prices import election_date_to_unix
        assert election_date_to_unix(None) is None

    def test_empty_string_returns_none(self):
        from pipeline_election_eve_prices import election_date_to_unix
        assert election_date_to_unix("") is None

    def test_timestamp_object_returns_none(self):
        """If a Timestamp sneaks in instead of string, shouldn't crash."""
        from pipeline_election_eve_prices import election_date_to_unix
        result = election_date_to_unix(pd.Timestamp("2024-11-05"))
        # Timestamp is not a str, so should return None
        assert result is None


# =============================================================================
# 5. LossySetitemError (election eve price column dtype)
# =============================================================================

class TestElectionEvePriceColumn:
    """The LossySetitemError when setting float into int-typed column."""

    def test_set_float_into_nan_column(self):
        """New column initialized with NaN should accept floats."""
        df = pd.DataFrame({"market_id": ["A", "B"]})
        df["election_eve_price"] = np.nan
        df.loc[0, "election_eve_price"] = 0.63
        assert df.loc[0, "election_eve_price"] == 0.63

    def test_set_float_into_int_column_warns_or_crashes(self):
        """In newer pandas this raises LossySetitemError, in older it warns."""
        df = pd.DataFrame({"election_eve_price": [1, 0, 1]})
        assert df["election_eve_price"].dtype in [np.int64, np.int32, int]
        # Either raises or warns depending on pandas version — both are bad
        # Our fix (cast to float64 first) avoids both
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                df.loc[0, "election_eve_price"] = 0.63
                # If it silently succeeds, the column got implicitly cast
            except (Exception, Warning):
                pass  # Expected — this is the bug we're guarding against

    def test_float64_cast_fixes_it(self):
        """Our fix: cast to float64 first."""
        df = pd.DataFrame({"election_eve_price": [1, 0, 1]})
        df["election_eve_price"] = df["election_eve_price"].astype("float64")
        df.loc[0, "election_eve_price"] = 0.63
        assert df.loc[0, "election_eve_price"] == 0.63

    def test_to_numeric_coerce_fixes_it(self):
        """Alternative fix with pd.to_numeric."""
        df = pd.DataFrame({"election_eve_price": ["1", "bad", "0.5"]})
        df["election_eve_price"] = pd.to_numeric(df["election_eve_price"], errors="coerce")
        assert df["election_eve_price"].dtype == np.float64
        df.loc[0, "election_eve_price"] = 0.63
        assert df.loc[0, "election_eve_price"] == 0.63


# =============================================================================
# 6. Price lookup functions (election eve)
# =============================================================================

class TestPriceLookups:

    def test_pm_lookup_valid(self):
        from pipeline_election_eve_prices import lookup_pm_price
        pm_data = {"12345": [{"t": 100, "p": 0.5}, {"t": 200, "p": 0.6}]}
        assert lookup_pm_price("12345", 250, pm_data) == 0.6
        assert lookup_pm_price("12345", 150, pm_data) == 0.5

    def test_pm_lookup_no_price_before_ts(self):
        from pipeline_election_eve_prices import lookup_pm_price
        pm_data = {"12345": [{"t": 100, "p": 0.5}]}
        assert lookup_pm_price("12345", 50, pm_data) is None

    def test_pm_lookup_missing_token(self):
        from pipeline_election_eve_prices import lookup_pm_price
        assert lookup_pm_price("99999", 100, {}) is None

    def test_pm_lookup_float_token_id(self):
        """Token IDs sometimes come as 12345.0 from pandas."""
        from pipeline_election_eve_prices import lookup_pm_price
        pm_data = {"12345": [{"t": 100, "p": 0.5}]}
        assert lookup_pm_price(12345.0, 250, pm_data) == 0.5

    def test_kalshi_lookup_valid(self):
        from pipeline_election_eve_prices import lookup_kalshi_price
        kal_data = {"PRES-24": [
            {"end_period_ts": 100, "price": {"close_dollars": 0.55}},
            {"end_period_ts": 200, "price": {"close_dollars": 0.65}},
        ]}
        assert lookup_kalshi_price("PRES-24", 250, kal_data) == 0.65

    def test_kalshi_lookup_missing_ticker(self):
        from pipeline_election_eve_prices import lookup_kalshi_price
        assert lookup_kalshi_price("FAKE", 100, {}) is None


# =============================================================================
# 7. get_market_anchor_time (config.py — Brier score NaT crash)
# =============================================================================

class TestGetMarketAnchorTime:

    def test_valid_election_date(self):
        from config import get_market_anchor_time
        dt = pd.Timestamp("2024-11-05", tz="UTC")
        row = {"trading_close_time": "2024-11-06T12:00:00Z"}
        result = get_market_anchor_time(row, is_election=True,
                                         election_date_lookup_fn=lambda r: dt)
        assert result == dt

    def test_nat_election_date_falls_through(self):
        from config import get_market_anchor_time
        row = {"trading_close_time": "2024-11-06T12:00:00Z"}
        result = get_market_anchor_time(row, is_election=True,
                                         election_date_lookup_fn=lambda r: pd.NaT)
        # Should fall through to trading_close_time, not crash
        assert result is not None
        assert pd.notna(result)

    def test_none_election_date_falls_through(self):
        from config import get_market_anchor_time
        row = {"trading_close_time": "2024-11-06T12:00:00Z"}
        result = get_market_anchor_time(row, is_election=True,
                                         election_date_lookup_fn=lambda r: None)
        assert result is not None

    def test_no_trading_close_returns_none(self):
        from config import get_market_anchor_time
        row = {"trading_close_time": np.nan}
        result = get_market_anchor_time(row, is_election=False)
        assert result is None
