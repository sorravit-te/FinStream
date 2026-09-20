from datetime import date
from decimal import Decimal

import pytest

from finstream.fred.models import FredObservation, FredSeriesObservations
from finstream.fred.parsing import (
    FredObservationValidationError,
    parse_series_observations,
)


def _observation(**overrides: object) -> dict[str, object]:
    observation: dict[str, object] = {
        "realtime_start": "2026-08-29",
        "realtime_end": "2026-08-29",
        "date": "2026-08-28",
        "value": "4.33",
        "future_observation_field": {"ignored": True},
    }
    observation.update(overrides)
    return observation


def _payload(observations: list[object] | None = None) -> dict[str, object]:
    return {
        "observations": [_observation()] if observations is None else observations,
        "future_top_level_field": "ignored",
    }


def test_parses_numeric_observation_and_ignores_unknown_fields() -> None:
    result = parse_series_observations(
        _payload(),
        expected_series_id="DFF",
    )

    assert result == FredSeriesObservations(
        series_id="DFF",
        observations=(
            FredObservation(
                series_id="DFF",
                realtime_start=date(2026, 8, 29),
                realtime_end=date(2026, 8, 29),
                observation_date=date(2026, 8, 28),
                value=Decimal("4.33"),
            ),
        ),
    )


@pytest.mark.parametrize(
    ("source_value", "expected_value"),
    [
        ("0", Decimal("0")),
        ("-0.25", Decimal("-0.25")),
    ],
)
def test_parses_zero_and_negative_values(
    source_value: str,
    expected_value: Decimal,
) -> None:
    result = parse_series_observations(
        _payload([_observation(value=source_value)]),
        expected_series_id="DFF",
    )

    assert result.observations[0].value == expected_value


def test_preserves_decimal_precision_without_float_conversion() -> None:
    source_value = "12345.6789000000000000001"

    result = parse_series_observations(
        _payload([_observation(value=source_value)]),
        expected_series_id="DFF",
    )

    assert result.observations[0].value == Decimal(source_value)


def test_multiple_observations_preserve_provider_order_and_series_id() -> None:
    result = parse_series_observations(
        _payload(
            [
                _observation(date="2026-03-01", value="3"),
                _observation(date="2026-01-01", value="1"),
                _observation(date="2026-02-01", value="2"),
            ]
        ),
        expected_series_id="DFF",
    )

    assert result.series_id == "DFF"
    assert [item.series_id for item in result.observations] == [
        "DFF",
        "DFF",
        "DFF",
    ]
    assert [item.observation_date for item in result.observations] == [
        date(2026, 3, 1),
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]


@pytest.mark.parametrize(
    ("first_value", "second_value"),
    [
        ("4.33", "4.33"),
        ("4.33", "4.34"),
        (".", "4.33"),
    ],
    ids=["same-value", "different-values", "missing-and-numeric"],
)
def test_rejects_duplicate_represented_occurrence_within_one_payload(
    first_value: str,
    second_value: str,
) -> None:
    with pytest.raises(
        FredObservationValidationError,
        match="duplicate observation occurrence for series_id=DFF",
    ):
        parse_series_observations(
            _payload(
                [
                    _observation(value=first_value),
                    _observation(value=second_value),
                ]
            ),
            expected_series_id="DFF",
        )


def test_allows_same_observation_date_with_distinct_realtime_start() -> None:
    result = parse_series_observations(
        _payload(
            [
                _observation(),
                _observation(realtime_start="2026-08-28"),
            ]
        ),
        expected_series_id="DFF",
    )

    assert [item.realtime_start for item in result.observations] == [
        date(2026, 8, 29),
        date(2026, 8, 28),
    ]


def test_allows_same_observation_date_with_distinct_realtime_end() -> None:
    result = parse_series_observations(
        _payload(
            [
                _observation(),
                _observation(realtime_end="2026-08-30"),
            ]
        ),
        expected_series_id="DFF",
    )

    assert [item.realtime_end for item in result.observations] == [
        date(2026, 8, 29),
        date(2026, 8, 30),
    ]


def test_duplicate_validation_is_scoped_to_one_payload() -> None:
    payload = _payload([_observation()])

    first_result = parse_series_observations(payload, expected_series_id="DFF")
    second_result = parse_series_observations(payload, expected_series_id="DFF")

    assert first_result == second_result


def test_missing_marker_becomes_none_without_dropping_observation() -> None:
    result = parse_series_observations(
        _payload([_observation(value=".")]),
        expected_series_id="DFF",
    )

    assert len(result.observations) == 1
    assert result.observations[0].value is None


def test_mixed_numeric_and_missing_observations_preserve_all_rows() -> None:
    result = parse_series_observations(
        _payload(
            [
                _observation(date="2026-08-26", value="4.31"),
                _observation(date="2026-08-27", value="."),
                _observation(date="2026-08-28", value="4.33"),
            ]
        ),
        expected_series_id="DFF",
    )

    assert tuple(item.value for item in result.observations) == (
        Decimal("4.31"),
        None,
        Decimal("4.33"),
    )


def test_empty_observations_is_valid_no_new_data_result() -> None:
    result = parse_series_observations(
        _payload([]),
        expected_series_id="DFF",
    )

    assert result == FredSeriesObservations(
        series_id="DFF",
        observations=(),
    )


@pytest.mark.parametrize("expected_series_id", ["", "   ", None])
def test_rejects_invalid_expected_series_id(expected_series_id: object) -> None:
    with pytest.raises(
        FredObservationValidationError,
        match="expected_series_id",
    ):
        parse_series_observations(
            _payload(),
            expected_series_id=expected_series_id,  # type: ignore[arg-type]
        )


def test_normalizes_series_id_whitespace_without_changing_case() -> None:
    result = parse_series_observations(
        _payload(),
        expected_series_id=" dFf ",
    )

    assert result.series_id == "dFf"
    assert result.observations[0].series_id == "dFf"


def test_rejects_non_object_payload() -> None:
    with pytest.raises(FredObservationValidationError, match="payload"):
        parse_series_observations(
            [],  # type: ignore[arg-type]
            expected_series_id="DFF",
        )


def test_rejects_missing_observations_collection() -> None:
    with pytest.raises(FredObservationValidationError, match="observations"):
        parse_series_observations({}, expected_series_id="DFF")


def test_rejects_non_list_observations_collection() -> None:
    with pytest.raises(FredObservationValidationError, match="observations"):
        parse_series_observations(
            {"observations": {}},
            expected_series_id="DFF",
        )


def test_rejects_non_object_observation_item() -> None:
    with pytest.raises(FredObservationValidationError, match="observation item"):
        parse_series_observations(
            _payload(["invalid"]),
            expected_series_id="DFF",
        )


def test_rejects_missing_observation_date() -> None:
    observation = _observation()
    del observation["date"]

    with pytest.raises(FredObservationValidationError, match="date"):
        parse_series_observations(
            _payload([observation]),
            expected_series_id="DFF",
        )


@pytest.mark.parametrize("observation_date", ["2026/08/28", "2026-02-30"])
def test_rejects_invalid_observation_date(observation_date: str) -> None:
    with pytest.raises(FredObservationValidationError, match="date"):
        parse_series_observations(
            _payload([_observation(date=observation_date)]),
            expected_series_id="DFF",
        )


def test_rejects_malformed_realtime_date() -> None:
    with pytest.raises(FredObservationValidationError, match="realtime_start"):
        parse_series_observations(
            _payload([_observation(realtime_start="2026/08/29")]),
            expected_series_id="DFF",
        )


def test_rejects_reversed_realtime_range() -> None:
    with pytest.raises(FredObservationValidationError, match="realtime_start"):
        parse_series_observations(
            _payload(
                [
                    _observation(
                        realtime_start="2026-08-30",
                        realtime_end="2026-08-29",
                    )
                ]
            ),
            expected_series_id="DFF",
        )


def test_rejects_non_string_value() -> None:
    with pytest.raises(FredObservationValidationError, match="value"):
        parse_series_observations(
            _payload([_observation(value=4.33)]),
            expected_series_id="DFF",
        )


@pytest.mark.parametrize(
    "value",
    ["", "   ", "not-a-number", "NaN", "Infinity", "-Infinity"],
)
def test_rejects_invalid_numeric_strings(value: str) -> None:
    with pytest.raises(FredObservationValidationError, match="value"):
        parse_series_observations(
            _payload([_observation(value=value)]),
            expected_series_id="DFF",
        )
