"""Validation and parsing for FRED series metadata payloads."""

import re
from datetime import date, datetime
from typing import Any

from finstream.fred.models import FredSeriesMetadata


_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")


class FredSeriesMetadataValidationError(ValueError):
    """Raised when a FRED series metadata payload is invalid."""


def _parse_required_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FredSeriesMetadataValidationError(
            f"{field_name} must be a non-blank string"
        )
    return value.strip()


def _parse_date(value: object, *, field_name: str) -> date:
    if not isinstance(value, str) or not _DATE_PATTERN.fullmatch(value):
        raise FredSeriesMetadataValidationError(
            f"{field_name} must use YYYY-MM-DD format"
        )
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise FredSeriesMetadataValidationError(
            f"{field_name} is not a valid date"
        ) from exc


def _parse_last_updated(value: object) -> datetime:
    text_value = _parse_required_string(value, field_name="last_updated")
    try:
        parsed_value = datetime.fromisoformat(text_value)
    except ValueError as exc:
        raise FredSeriesMetadataValidationError(
            "last_updated is not a valid ISO-8601 timestamp"
        ) from exc
    if parsed_value.tzinfo is None or parsed_value.utcoffset() is None:
        raise FredSeriesMetadataValidationError(
            "last_updated must include a timezone"
        )
    return parsed_value


def _parse_popularity(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FredSeriesMetadataValidationError(
            "popularity must be a non-negative integer"
        )
    return value


def _parse_notes(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise FredSeriesMetadataValidationError("notes must be a string or null")
    return value.strip() or None


def parse_series_metadata(
    payload: dict[str, Any],
    *,
    expected_series_id: str,
) -> FredSeriesMetadata:
    """Parse metadata for one specifically requested FRED series."""
    normalized_expected_id = _parse_required_string(
        expected_series_id,
        field_name="expected_series_id",
    )
    if not isinstance(payload, dict):
        raise FredSeriesMetadataValidationError("payload must be an object")

    series_values = payload.get("seriess")
    if not isinstance(series_values, list):
        raise FredSeriesMetadataValidationError("seriess must be a list")
    if len(series_values) != 1:
        raise FredSeriesMetadataValidationError(
            "seriess must contain exactly one series"
        )

    series_value = series_values[0]
    if not isinstance(series_value, dict):
        raise FredSeriesMetadataValidationError("series item must be an object")

    series_id = _parse_required_string(series_value.get("id"), field_name="id")
    if series_id != normalized_expected_id:
        raise FredSeriesMetadataValidationError(
            "provider series ID does not match expected_series_id"
        )

    realtime_start = _parse_date(
        series_value.get("realtime_start"),
        field_name="realtime_start",
    )
    realtime_end = _parse_date(
        series_value.get("realtime_end"),
        field_name="realtime_end",
    )
    if realtime_start > realtime_end:
        raise FredSeriesMetadataValidationError(
            "realtime_start must not be after realtime_end"
        )

    observation_start = _parse_date(
        series_value.get("observation_start"),
        field_name="observation_start",
    )
    observation_end = _parse_date(
        series_value.get("observation_end"),
        field_name="observation_end",
    )
    if observation_start > observation_end:
        raise FredSeriesMetadataValidationError(
            "observation_start must not be after observation_end"
        )

    return FredSeriesMetadata(
        series_id=series_id,
        realtime_start=realtime_start,
        realtime_end=realtime_end,
        title=_parse_required_string(series_value.get("title"), field_name="title"),
        observation_start=observation_start,
        observation_end=observation_end,
        frequency=_parse_required_string(
            series_value.get("frequency"),
            field_name="frequency",
        ),
        frequency_short=_parse_required_string(
            series_value.get("frequency_short"),
            field_name="frequency_short",
        ),
        units=_parse_required_string(series_value.get("units"), field_name="units"),
        units_short=_parse_required_string(
            series_value.get("units_short"),
            field_name="units_short",
        ),
        seasonal_adjustment=_parse_required_string(
            series_value.get("seasonal_adjustment"),
            field_name="seasonal_adjustment",
        ),
        seasonal_adjustment_short=_parse_required_string(
            series_value.get("seasonal_adjustment_short"),
            field_name="seasonal_adjustment_short",
        ),
        last_updated=_parse_last_updated(series_value.get("last_updated")),
        popularity=_parse_popularity(series_value.get("popularity")),
        notes=_parse_notes(series_value.get("notes")),
    )
