from datetime import date, datetime, timedelta, timezone

import pytest

from finstream.fred.models import FredSeriesMetadata
from finstream.fred.parsing import (
    FredSeriesMetadataValidationError,
    parse_series_metadata,
)


def _series(**overrides: object) -> dict[str, object]:
    series: dict[str, object] = {
        "id": "DFF",
        "realtime_start": "2026-08-01",
        "realtime_end": "2026-08-29",
        "title": "Federal Funds Effective Rate",
        "observation_start": "1954-07-01",
        "observation_end": "2026-08-28",
        "frequency": "Daily",
        "frequency_short": "D",
        "units": "Percent",
        "units_short": "%",
        "seasonal_adjustment": "Not Seasonally Adjusted",
        "seasonal_adjustment_short": "NSA",
        "last_updated": "2013-07-31 09:26:16-05",
        "popularity": 78,
        "notes": " Source notes. ",
        "future_provider_field": {"ignored": True},
    }
    series.update(overrides)
    return series


def _payload(*series: object) -> dict[str, object]:
    values = list(series) if series else [_series()]
    return {
        "realtime_start": "2026-08-01",
        "realtime_end": "2026-08-29",
        "seriess": values,
        "future_top_level_field": "ignored",
    }


def test_parses_complete_series_metadata() -> None:
    result = parse_series_metadata(_payload(), expected_series_id=" DFF ")

    assert result == FredSeriesMetadata(
        series_id="DFF",
        realtime_start=date(2026, 8, 1),
        realtime_end=date(2026, 8, 29),
        title="Federal Funds Effective Rate",
        observation_start=date(1954, 7, 1),
        observation_end=date(2026, 8, 28),
        frequency="Daily",
        frequency_short="D",
        units="Percent",
        units_short="%",
        seasonal_adjustment="Not Seasonally Adjusted",
        seasonal_adjustment_short="NSA",
        last_updated=datetime(
            2013,
            7,
            31,
            9,
            26,
            16,
            tzinfo=timezone(timedelta(hours=-5)),
        ),
        popularity=78,
        notes="Source notes.",
    )
    assert result.last_updated.utcoffset() == timedelta(hours=-5)


def test_missing_notes_becomes_none() -> None:
    series = _series()
    del series["notes"]

    assert parse_series_metadata(
        _payload(series),
        expected_series_id="DFF",
    ).notes is None


@pytest.mark.parametrize("notes", [None, "", "   "])
def test_null_or_blank_notes_becomes_none(notes: object) -> None:
    result = parse_series_metadata(
        _payload(_series(notes=notes)),
        expected_series_id="DFF",
    )

    assert result.notes is None


def test_rejects_non_string_notes() -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="notes"):
        parse_series_metadata(
            _payload(_series(notes=123)),
            expected_series_id="DFF",
        )


@pytest.mark.parametrize("expected_series_id", ["", "   ", None])
def test_rejects_blank_expected_series_id(expected_series_id: object) -> None:
    with pytest.raises(
        FredSeriesMetadataValidationError,
        match="expected_series_id",
    ):
        parse_series_metadata(
            _payload(),
            expected_series_id=expected_series_id,  # type: ignore[arg-type]
        )


def test_rejects_non_object_payload() -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="payload"):
        parse_series_metadata([], expected_series_id="DFF")  # type: ignore[arg-type]


def test_rejects_missing_seriess() -> None:
    payload = _payload()
    del payload["seriess"]

    with pytest.raises(FredSeriesMetadataValidationError, match="seriess"):
        parse_series_metadata(payload, expected_series_id="DFF")


def test_rejects_non_list_seriess() -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="seriess"):
        parse_series_metadata(
            {"seriess": {}},
            expected_series_id="DFF",
        )


@pytest.mark.parametrize("series", [[], [_series(), _series()]])
def test_rejects_series_count_other_than_one(series: list[object]) -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="exactly one"):
        parse_series_metadata(
            {"seriess": series},
            expected_series_id="DFF",
        )


def test_rejects_non_object_series_item() -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="series item"):
        parse_series_metadata(
            _payload("invalid"),
            expected_series_id="DFF",
        )


def test_rejects_provider_series_id_mismatch() -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="does not match"):
        parse_series_metadata(
            _payload(_series(id="UNRATE")),
            expected_series_id="DFF",
        )


def test_rejects_blank_provider_series_id() -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="id"):
        parse_series_metadata(
            _payload(_series(id="   ")),
            expected_series_id="DFF",
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("title", None),
        ("frequency", 12),
        ("units", "   "),
    ],
)
def test_rejects_invalid_required_text(field_name: str, value: object) -> None:
    series = _series(**{field_name: value})
    if value is None:
        del series[field_name]

    with pytest.raises(FredSeriesMetadataValidationError, match=field_name):
        parse_series_metadata(
            _payload(series),
            expected_series_id="DFF",
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("realtime_start", "2026/08/01"),
        ("observation_end", "2026-02-30"),
    ],
)
def test_rejects_invalid_dates(field_name: str, value: str) -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match=field_name):
        parse_series_metadata(
            _payload(_series(**{field_name: value})),
            expected_series_id="DFF",
        )


def test_rejects_reversed_realtime_range() -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="realtime_start"):
        parse_series_metadata(
            _payload(
                _series(
                    realtime_start="2026-08-29",
                    realtime_end="2026-08-01",
                )
            ),
            expected_series_id="DFF",
        )


def test_rejects_reversed_observation_range() -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="observation_start"):
        parse_series_metadata(
            _payload(
                _series(
                    observation_start="2026-08-29",
                    observation_end="2026-08-01",
                )
            ),
            expected_series_id="DFF",
        )


@pytest.mark.parametrize(
    "last_updated",
    ["not-a-timestamp", "2026-08-29 12:00:00"],
)
def test_rejects_invalid_or_timezone_naive_last_updated(last_updated: str) -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="last_updated"):
        parse_series_metadata(
            _payload(_series(last_updated=last_updated)),
            expected_series_id="DFF",
        )


@pytest.mark.parametrize("popularity", [True, 1.5, "1", -1])
def test_rejects_invalid_popularity(popularity: object) -> None:
    with pytest.raises(FredSeriesMetadataValidationError, match="popularity"):
        parse_series_metadata(
            _payload(_series(popularity=popularity)),
            expected_series_id="DFF",
        )
