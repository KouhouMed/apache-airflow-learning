"""
Unit tests for data quality and categorisation logic — Days 12 & 10.

These test the pure validation functions independently of Airflow and SQLite,
so they run locally with `pytest tests/` using only requirements-dev.txt.
The constants and helper functions are replicated here to avoid importing
the DAG files (which would require Airflow to be installed).
"""

import pytest

# ---------------------------------------------------------------------------
# Replicated from weather_fetch.py (Day 12)
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = [
    "temperature_2m", "apparent_temperature",
    "relative_humidity_2m", "wind_speed_10m", "weather_code", "time",
]

FIELD_RANGES = {
    "temperature_2m":       (-90.0,  60.0),
    "apparent_temperature": (-90.0,  70.0),
    "relative_humidity_2m": (  0.0, 100.0),
    "wind_speed_10m":       (  0.0, 500.0),
}


def _check_nulls(raw: dict) -> list[str]:
    """Return list of required fields that are None or missing."""
    return [f for f in REQUIRED_FIELDS if raw.get(f) is None]


def _check_ranges(raw: dict) -> list[str]:
    """Return list of field names whose values fall outside FIELD_RANGES."""
    violations = []
    for field, (lo, hi) in FIELD_RANGES.items():
        value = raw.get(field)
        if value is not None and not (lo <= value <= hi):
            violations.append(field)
    return violations


# ---------------------------------------------------------------------------
# Replicated from air_quality_pipeline.py (Day 10)
# ---------------------------------------------------------------------------

EU_AQI_CATEGORIES = [
    (20,          "Good"),
    (40,          "Fair"),
    (60,          "Moderate"),
    (80,          "Poor"),
    (100,         "Very Poor"),
    (float("inf"), "Extremely Poor"),
]

US_AQI_CATEGORIES = [
    (50,          "Good"),
    (100,         "Moderate"),
    (150,         "Unhealthy for Sensitive Groups"),
    (200,         "Unhealthy"),
    (300,         "Very Unhealthy"),
    (float("inf"), "Hazardous"),
]


def _categorize(value: float, scale: list) -> str:
    for threshold, label in scale:
        if value <= threshold:
            return label
    return "Unknown"


# ---------------------------------------------------------------------------
# Tests — null checks
# ---------------------------------------------------------------------------

class TestNullCheck:
    def test_all_present(self):
        raw = {
            "temperature_2m": 20.0, "apparent_temperature": 19.0,
            "relative_humidity_2m": 70, "wind_speed_10m": 10.0,
            "weather_code": 0, "time": "2026-08-30T12:00",
        }
        assert _check_nulls(raw) == []

    def test_single_missing_field(self):
        raw = {"temperature_2m": 20.0}  # five required fields absent
        nulls = _check_nulls(raw)
        assert "apparent_temperature" in nulls
        assert "time" in nulls
        assert "temperature_2m" not in nulls  # this one IS present

    def test_explicit_none_flagged(self):
        raw = {
            "temperature_2m": None, "apparent_temperature": 19.0,
            "relative_humidity_2m": 70, "wind_speed_10m": 10.0,
            "weather_code": 0, "time": "2026-08-30T12:00",
        }
        assert "temperature_2m" in _check_nulls(raw)

    def test_empty_payload_flags_all(self):
        assert len(_check_nulls({})) == len(REQUIRED_FIELDS)


# ---------------------------------------------------------------------------
# Tests — range checks
# ---------------------------------------------------------------------------

class TestRangeCheck:
    def _valid_payload(self):
        return {
            "temperature_2m": 22.0,
            "apparent_temperature": 21.0,
            "relative_humidity_2m": 65,
            "wind_speed_10m": 12.0,
        }

    def test_normal_paris_summer(self):
        assert _check_ranges(self._valid_payload()) == []

    def test_freezing_paris_winter(self):
        raw = {
            "temperature_2m": -8.0,
            "apparent_temperature": -12.0,
            "relative_humidity_2m": 85,
            "wind_speed_10m": 25.0,
        }
        assert _check_ranges(raw) == []

    def test_temperature_above_max(self):
        raw = {"temperature_2m": 61.0}
        assert "temperature_2m" in _check_ranges(raw)

    def test_temperature_below_min(self):
        raw = {"temperature_2m": -91.0}
        assert "temperature_2m" in _check_ranges(raw)

    def test_humidity_above_100(self):
        raw = {"relative_humidity_2m": 101.0}
        assert "relative_humidity_2m" in _check_ranges(raw)

    def test_negative_wind_speed(self):
        raw = {"wind_speed_10m": -1.0}
        assert "wind_speed_10m" in _check_ranges(raw)

    def test_none_values_are_skipped(self):
        """None values pass range check — null check handles them separately."""
        raw = {"temperature_2m": None, "wind_speed_10m": None}
        assert _check_ranges(raw) == []

    def test_exact_lower_boundary_accepted(self):
        raw = {
            "temperature_2m": -90.0,
            "relative_humidity_2m": 0.0,
            "wind_speed_10m": 0.0,
        }
        assert _check_ranges(raw) == []

    def test_exact_upper_boundary_accepted(self):
        raw = {
            "temperature_2m": 60.0,
            "relative_humidity_2m": 100.0,
            "wind_speed_10m": 500.0,
        }
        assert _check_ranges(raw) == []

    def test_multiple_violations_all_reported(self):
        raw = {
            "temperature_2m": 999.0,       # too high
            "relative_humidity_2m": -5.0,  # too low
        }
        violations = _check_ranges(raw)
        assert "temperature_2m" in violations
        assert "relative_humidity_2m" in violations


# ---------------------------------------------------------------------------
# Tests — AQI categorisation
# ---------------------------------------------------------------------------

class TestEUAQI:
    def test_good(self):
        assert _categorize(10, EU_AQI_CATEGORIES) == "Good"

    def test_boundary_is_inclusive(self):
        assert _categorize(20, EU_AQI_CATEGORIES) == "Good"
        assert _categorize(21, EU_AQI_CATEGORIES) == "Fair"

    def test_fair(self):
        assert _categorize(35, EU_AQI_CATEGORIES) == "Fair"

    def test_moderate(self):
        assert _categorize(55, EU_AQI_CATEGORIES) == "Moderate"

    def test_poor(self):
        assert _categorize(75, EU_AQI_CATEGORIES) == "Poor"

    def test_very_poor(self):
        assert _categorize(95, EU_AQI_CATEGORIES) == "Very Poor"

    def test_extremely_poor(self):
        assert _categorize(200, EU_AQI_CATEGORIES) == "Extremely Poor"

    def test_zero_is_good(self):
        assert _categorize(0, EU_AQI_CATEGORIES) == "Good"


class TestUSAQI:
    def test_good(self):
        assert _categorize(25, US_AQI_CATEGORIES) == "Good"

    def test_moderate(self):
        assert _categorize(75, US_AQI_CATEGORIES) == "Moderate"

    def test_unhealthy_for_sensitive(self):
        assert _categorize(125, US_AQI_CATEGORIES) == "Unhealthy for Sensitive Groups"

    def test_unhealthy(self):
        assert _categorize(175, US_AQI_CATEGORIES) == "Unhealthy"

    def test_very_unhealthy(self):
        assert _categorize(250, US_AQI_CATEGORIES) == "Very Unhealthy"

    def test_hazardous(self):
        assert _categorize(400, US_AQI_CATEGORIES) == "Hazardous"

    def test_exact_boundary_50(self):
        assert _categorize(50, US_AQI_CATEGORIES) == "Good"
        assert _categorize(51, US_AQI_CATEGORIES) == "Moderate"
