from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import psycopg
import pytest

from finstream.quality.monitoring import (
    QualityStatus,
    monitor_fred_observation_freshness,
    monitor_market_freshness,
    monitor_run_count_movement,
    monitor_sec_company_facts_freshness,
    monitor_sec_submissions_freshness,
)


_AS_OF = date(2026, 9, 21)


def _connection() -> tuple[MagicMock, MagicMock]:
    connection = MagicMock(spec=psycopg.Connection)
    cursor = connection.cursor.return_value.__enter__.return_value
    return connection, cursor


def _measurement_values(signal: object) -> dict[str, object]:
    return {
        measurement.name: measurement.value
        for measurement in signal.measurements  # type: ignore[union-attr]
    }


def _assert_caller_owns_transaction(connection: MagicMock) -> None:
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


def test_market_recency_uses_explicit_as_of_and_is_symbol_isolated() -> None:
    connection, cursor = _connection()
    cursor.fetchone.return_value = (date(2026, 9, 18),)

    signal = monitor_market_freshness(connection, " aapl ", as_of=_AS_OF)

    assert signal.status is QualityStatus.INFO
    assert signal.entity_id == "AAPL"
    assert _measurement_values(signal) == {
        "latest_date": date(2026, 9, 18),
        "age_calendar_days": 3,
    }
    assert cursor.execute.call_args.args[1] == (
        "twelve_data",
        "daily_market_prices",
        "AAPL",
    )
    _assert_caller_owns_transaction(connection)


def test_market_weekend_age_is_informational_without_calendar_policy() -> None:
    connection, cursor = _connection()
    cursor.fetchone.return_value = (date(2026, 9, 18),)

    signal = monitor_market_freshness(
        connection,
        "AAPL",
        as_of=date(2026, 9, 20),
    )

    assert signal.status is QualityStatus.INFO
    assert _measurement_values(signal)["age_calendar_days"] == 2
    assert "exchange calendar" in signal.message


def test_market_no_history_is_informational() -> None:
    connection, cursor = _connection()
    cursor.fetchone.return_value = (None,)

    signal = monitor_market_freshness(connection, "AAPL", as_of=_AS_OF)

    assert signal.status is QualityStatus.INFO
    assert _measurement_values(signal) == {
        "latest_date": None,
        "age_calendar_days": None,
    }


def test_market_invalid_measurement_input_returns_error_without_query() -> None:
    connection, _ = _connection()

    signal = monitor_market_freshness(connection, " ", as_of=_AS_OF)

    assert signal.status is QualityStatus.ERROR
    connection.cursor.assert_not_called()
    _assert_caller_owns_transaction(connection)


def test_fred_recency_is_series_isolated_and_value_independent() -> None:
    connection, cursor = _connection()
    cursor.fetchone.return_value = (date(2026, 8, 31),)

    signal = monitor_fred_observation_freshness(connection, " DFF ", as_of=_AS_OF)

    assert signal.status is QualityStatus.INFO
    assert signal.entity_id == "DFF"
    assert _measurement_values(signal) == {
        "latest_date": date(2026, 8, 31),
        "age_calendar_days": 21,
    }
    assert cursor.execute.call_args.args[1] == (
        "fred",
        "series_observations",
        "DFF",
    )
    assert "cadence threshold" in signal.message
    _assert_caller_owns_transaction(connection)


def test_fred_no_history_is_informational() -> None:
    connection, cursor = _connection()
    cursor.fetchone.return_value = (None,)

    signal = monitor_fred_observation_freshness(connection, "DFF", as_of=_AS_OF)

    assert signal.status is QualityStatus.INFO
    assert _measurement_values(signal)["latest_date"] is None
    _assert_caller_owns_transaction(connection)


@pytest.mark.parametrize(
    ("monitor", "dataset", "expected_check_id", "latest_date"),
    [
        (
            monitor_sec_submissions_freshness,
            "submissions",
            "sec_latest_submissions_filing_date",
            date(2026, 9, 1),
        ),
        (
            monitor_sec_company_facts_freshness,
            "company_facts",
            "sec_latest_company_facts_filed_date",
            date(2026, 8, 15),
        ),
    ],
)
def test_sec_filing_recency_is_informational(
    monitor: object,
    dataset: str,
    expected_check_id: str,
    latest_date: date,
) -> None:
    connection, cursor = _connection()
    cursor.fetchone.return_value = (latest_date,)

    signal = monitor(  # type: ignore[operator]
        connection,
        "320193",
        as_of=_AS_OF,
    )

    assert signal.status is QualityStatus.INFO
    assert signal.check_id == expected_check_id
    assert signal.entity_id == "0000320193"
    assert _measurement_values(signal)["latest_date"] == latest_date
    assert cursor.execute.call_args.args[1] == ("sec_edgar", dataset, "0000320193")
    assert "event-driven" in signal.message
    _assert_caller_owns_transaction(connection)


def test_sec_no_history_is_informational() -> None:
    connection, cursor = _connection()
    cursor.fetchone.return_value = (None,)

    signal = monitor_sec_submissions_freshness(
        connection,
        "0000320193",
        as_of=_AS_OF,
    )

    assert signal.status is QualityStatus.INFO
    assert _measurement_values(signal)["latest_date"] is None


def test_run_count_movement_measures_delta_without_warning() -> None:
    connection, cursor = _connection()
    cursor.fetchall.return_value = [
        (8, datetime(2026, 9, 20, 12, tzinfo=timezone.utc), "20260920T120000000000Z"),
        (4, datetime(2026, 9, 19, 12, tzinfo=timezone.utc), "20260919T120000000000Z"),
    ]

    signal = monitor_run_count_movement(
        connection,
        source="fred",
        dataset="series_observations",
        as_of=_AS_OF,
    )

    assert signal.status is QualityStatus.INFO
    assert _measurement_values(signal) == {
        "current_record_count": 8,
        "current_ingested_at": datetime(2026, 9, 20, 12, tzinfo=timezone.utc),
        "current_run_id": "20260920T120000000000Z",
        "previous_record_count": 4,
        "absolute_delta": 4,
        "percent_delta": 100.0,
        "scope_comparable": False,
        "previous_ingested_at": datetime(2026, 9, 19, 12, tzinfo=timezone.utc),
        "previous_run_id": "20260919T120000000000Z",
    }
    assert cursor.execute.call_args.args[1] == ("fred", "series_observations")
    assert "scope comparability" in signal.message
    _assert_caller_owns_transaction(connection)


def test_run_count_movement_handles_zero_previous_count_without_percentage() -> None:
    connection, cursor = _connection()
    cursor.fetchall.return_value = [
        (3, datetime(2026, 9, 20, 12, tzinfo=timezone.utc), "20260920T120000000000Z"),
        (0, datetime(2026, 9, 19, 12, tzinfo=timezone.utc), "20260919T120000000000Z"),
    ]

    signal = monitor_run_count_movement(
        connection,
        source="sec_edgar",
        dataset="submissions",
        as_of=_AS_OF,
    )

    assert signal.status is QualityStatus.INFO
    assert _measurement_values(signal)["absolute_delta"] == 3
    assert _measurement_values(signal)["percent_delta"] is None
    assert "undefined" in signal.message


def test_run_count_movement_without_history_is_informational() -> None:
    connection, cursor = _connection()
    cursor.fetchall.return_value = []

    signal = monitor_run_count_movement(
        connection,
        source="fred",
        dataset="series_metadata",
        as_of=_AS_OF,
    )

    assert signal.status is QualityStatus.INFO
    assert _measurement_values(signal) == {
        "current_record_count": None,
        "previous_record_count": None,
        "scope_comparable": False,
    }
    _assert_caller_owns_transaction(connection)


def test_monitoring_requires_a_date_as_of_value() -> None:
    connection, _ = _connection()

    with pytest.raises(ValueError, match="as_of"):
        monitor_market_freshness(
            connection,
            "AAPL",
            as_of=datetime(2026, 9, 21, tzinfo=timezone.utc),
        )
