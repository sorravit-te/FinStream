from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, Mock, call

import psycopg
import pytest

from finstream.bronze.parquet_storage import read_parquet
from finstream.fred.client import FredClient
from finstream.fred.ingestion import (
    FRED_INCREMENTAL_OVERLAP_DAYS,
    FredMacroeconomicIngestionService,
    fred_incremental_observation_start_date,
)
from finstream.fred.postgres import latest_fred_observation_date


_RUN_AT = datetime(2026, 9, 19, 12, 0, 0, tzinfo=timezone.utc)


def _metadata_payload(series_id: str = "DFF") -> dict[str, object]:
    return {
        "seriess": [
            {
                "id": series_id,
                "realtime_start": "2026-09-01",
                "realtime_end": "2026-09-19",
                "title": f"{series_id} title",
                "observation_start": "1954-07-01",
                "observation_end": "2026-09-01",
                "frequency": "Daily",
                "frequency_short": "D",
                "units": "Percent",
                "units_short": "%",
                "seasonal_adjustment": "Not Seasonally Adjusted",
                "seasonal_adjustment_short": "NSA",
                "last_updated": "2026-09-19 12:00:00+00:00",
                "popularity": 1,
                "notes": None,
            }
        ]
    }


def _observation(
    observation_date: str,
    *,
    value: str,
    realtime_start: str = "2026-09-19",
    realtime_end: str = "2026-09-19",
) -> dict[str, str]:
    return {
        "date": observation_date,
        "value": value,
        "realtime_start": realtime_start,
        "realtime_end": realtime_end,
    }


def _observations_payload(rows: list[dict[str, str]] | None = None) -> dict[str, object]:
    return {"observations": [] if rows is None else rows}


def _watermark_connection(
    latest_observation_date: date | None,
) -> tuple[MagicMock, MagicMock]:
    connection = MagicMock(spec=psycopg.Connection)
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (latest_observation_date,)
    return connection, cursor


def _client(
    observations: dict[str, object],
) -> Mock:
    client = Mock(spec=FredClient)
    client.fetch_series.return_value = _metadata_payload()
    client.fetch_series_observations.return_value = observations
    return client


def test_fred_observation_watermark_returns_none_without_history() -> None:
    connection, cursor = _watermark_connection(None)

    assert latest_fred_observation_date(connection, " DFF ") is None

    cursor.execute.assert_called_once_with(
        """
    SELECT max(observation_date)
    FROM source_data.fred_series_observations
    WHERE source = %s AND dataset = %s AND series_id = %s
""",
        ("fred", "series_observations", "DFF"),
    )
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


def test_fred_observation_watermark_returns_exact_maximum_for_requested_series() -> None:
    connection, cursor = _watermark_connection(date(2026, 9, 1))

    assert latest_fred_observation_date(connection, "CPIAUCSL") == date(2026, 9, 1)

    assert cursor.execute.call_args.args[1] == (
        "fred",
        "series_observations",
        "CPIAUCSL",
    )
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()


@pytest.mark.parametrize(
    ("latest_observation_date", "overlap_days", "expected"),
    [
        (None, FRED_INCREMENTAL_OVERLAP_DAYS, None),
        (date(2026, 9, 1), 365, date(2025, 9, 1)),
        (date(2026, 9, 1), 0, date(2026, 9, 1)),
    ],
)
def test_fred_incremental_start_date_uses_calendar_day_arithmetic(
    latest_observation_date: date | None,
    overlap_days: int,
    expected: date | None,
) -> None:
    assert (
        fred_incremental_observation_start_date(
            latest_observation_date,
            overlap_days=overlap_days,
        )
        == expected
    )


@pytest.mark.parametrize("overlap_days", [-1, True, "365"])
def test_fred_incremental_start_date_rejects_invalid_overlap(
    overlap_days: object,
) -> None:
    with pytest.raises(ValueError):
        fred_incremental_observation_start_date(
            date(2026, 9, 1),
            overlap_days=overlap_days,  # type: ignore[arg-type]
        )


def test_incremental_fred_bootstrap_fetches_full_metadata_and_observations(
    tmp_path: Path,
) -> None:
    connection, _ = _watermark_connection(None)
    client = _client(_observations_payload([_observation("2026-09-01", value="5.0")]))

    result = FredMacroeconomicIngestionService(
        client
    ).ingest_series_incrementally_to_bronze(
        connection,
        " DFF ",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert result.series_id == "DFF"
    assert result.metadata.record_count == 1
    assert result.observations.record_count == 1
    assert client.method_calls == [
        call.fetch_series("DFF"),
        call.fetch_series_observations(
            "DFF",
            observation_start=None,
            observation_end=None,
            realtime_start=None,
            realtime_end=None,
        ),
    ]


def test_incremental_fred_uses_watermark_overlap_and_full_metadata(
    tmp_path: Path,
) -> None:
    connection, _ = _watermark_connection(date(2026, 9, 1))
    client = _client(_observations_payload([_observation("2026-09-01", value="5.1")]))

    result = FredMacroeconomicIngestionService(
        client
    ).ingest_series_incrementally_to_bronze(
        connection,
        "DFF",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert result.metadata.record_count == 1
    assert result.observations.record_count == 1
    assert client.method_calls == [
        call.fetch_series("DFF"),
        call.fetch_series_observations(
            "DFF",
            observation_start=date(2025, 9, 1),
            observation_end=None,
            realtime_start=None,
            realtime_end=None,
        ),
    ]


def test_explicit_fred_observation_bounds_override_automatic_watermark(
    tmp_path: Path,
) -> None:
    connection = MagicMock(spec=psycopg.Connection)
    client = _client(_observations_payload([_observation("2020-06-01", value="4.0")]))

    result = FredMacroeconomicIngestionService(
        client
    ).ingest_series_incrementally_to_bronze(
        connection,
        "DFF",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
        observation_start=date(2020, 1, 1),
        observation_end=date(2020, 12, 31),
    )

    assert result.observations.record_count == 1
    connection.cursor.assert_not_called()
    client.fetch_series_observations.assert_called_once_with(
        "DFF",
        observation_start=date(2020, 1, 1),
        observation_end=date(2020, 12, 31),
        realtime_start=None,
        realtime_end=None,
    )


def test_incremental_fred_preserves_revised_values_and_realtime_contexts(
    tmp_path: Path,
) -> None:
    connection, _ = _watermark_connection(date(2026, 9, 1))
    client = _client(
        _observations_payload(
            [
                _observation(
                    "2026-09-01",
                    value="5.2",
                    realtime_start="2026-09-19",
                    realtime_end="2026-09-19",
                ),
                _observation(
                    "2026-09-01",
                    value="5.0",
                    realtime_start="2026-09-01",
                    realtime_end="2026-09-18",
                ),
            ]
        )
    )

    result = FredMacroeconomicIngestionService(
        client
    ).ingest_series_incrementally_to_bronze(
        connection,
        "DFF",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    table = read_parquet(result.observations.location)
    assert result.observations.record_count == 2
    assert table.column("observation_date").to_pylist() == [
        date(2026, 9, 1),
        date(2026, 9, 1),
    ]
    assert table.column("realtime_start").to_pylist() == [
        date(2026, 9, 19),
        date(2026, 9, 1),
    ]
    assert table.column("realtime_end").to_pylist() == [
        date(2026, 9, 19),
        date(2026, 9, 18),
    ]
    assert table.column("value").to_pylist() == [Decimal("5.2"), Decimal("5.0")]


def test_incremental_fred_zero_observations_remain_valid(
    tmp_path: Path,
) -> None:
    connection, _ = _watermark_connection(date(2026, 9, 1))
    client = _client(_observations_payload())

    result = FredMacroeconomicIngestionService(
        client
    ).ingest_series_incrementally_to_bronze(
        connection,
        "DFF",
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert result.metadata.record_count == 1
    assert result.observations.record_count == 0
    assert read_parquet(result.observations.location).num_rows == 0
