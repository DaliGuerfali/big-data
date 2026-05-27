"""
Unit tests for get_week_boundaries (pure date logic, no Airflow needed).
"""

from dag_utils import get_week_boundaries


class TestGetWeekBoundaries:
    def _call(self, date_str: str) -> dict:
        return get_week_boundaries(reference_date=date_str)

    def test_midweek_date(self):
        # 2024-01-17 is a Wednesday in ISO week 3 of 2024
        result = self._call("2024-01-17")
        assert result["year"] == "2024"
        assert result["week"] == "03"
        assert result["week_number"] == "2024-03"
        assert result["start"] == "2024-01-15"  # Monday
        assert result["end"] == "2024-01-21"    # Sunday

    def test_monday(self):
        # 2024-01-15 is a Monday — start == reference date
        result = self._call("2024-01-15")
        assert result["start"] == "2024-01-15"
        assert result["end"] == "2024-01-21"

    def test_sunday(self):
        # 2024-01-21 is a Sunday — end == reference date
        result = self._call("2024-01-21")
        assert result["start"] == "2024-01-15"
        assert result["end"] == "2024-01-21"

    def test_year_boundary(self):
        # 2024-01-01 (Monday) is in ISO week 1 of 2024
        result = self._call("2024-01-01")
        assert result["year"] == "2024"
        assert result["week"] == "01"
        assert result["start"] == "2024-01-01"

    def test_week_zero_padding(self):
        # Week numbers below 10 should be zero-padded to 2 digits
        result = self._call("2024-01-10")
        assert len(result["week"]) == 2
        assert result["week"].startswith("0")

    def test_return_keys(self):
        result = self._call("2024-06-15")
        assert set(result.keys()) == {"week_number", "start", "end", "year", "week"}
