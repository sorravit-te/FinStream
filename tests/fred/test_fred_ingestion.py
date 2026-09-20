from datetime import date
from unittest.mock import Mock, call

import pytest

from finstream.fred.client import FredClient, FredError
from finstream.fred.ingestion import FredMacroeconomicIngestionService
from finstream.fred.models import (
    FredSeriesMetadata,
    FredSeriesObservations,
    FredSeriesSourceData,
)
from finstream.fred.parsing import (
    FredObservationValidationError,
    FredSeriesMetadataValidationError,
)
from finstream.fred.series import INITIAL_FRED_SERIES_IDS


def _metadata_payload(series_id: str) -> dict[str, object]:
    return {
        "seriess": [
            {
                "id": series_id,
                "realtime_start": "2026-08-01",
                "realtime_end": "2026-08-29",
                "title": f"{series_id} title",
                "observation_start": "2000-01-01",
                "observation_end": "2026-08-28",
                "frequency": "Source frequency",
                "frequency_short": "F",
                "units": "Source units",
                "units_short": "U",
                "seasonal_adjustment": "Source adjustment",
                "seasonal_adjustment_short": "SA",
                "last_updated": "2026-08-29 12:00:00+00",
                "popularity": 1,
            }
        ]
    }


def _observations_payload(*, empty: bool = False) -> dict[str, object]:
    observations: list[object] = []
    if not empty:
        observations.append(
            {
                "realtime_start": "2026-08-29",
                "realtime_end": "2026-08-29",
                "date": "2026-08-28",
                "value": "4.33",
            }
        )
    return {"observations": observations}


def _valid_client() -> Mock:
    client = Mock(spec=FredClient)
    client.fetch_series.side_effect = _metadata_payload
    client.fetch_series_observations.side_effect = (
        lambda series_id, **kwargs: _observations_payload()
    )
    return client


def _observation_call(
    series_id: str,
    *,
    observation_start: date | None = None,
    observation_end: date | None = None,
    realtime_start: date | None = None,
    realtime_end: date | None = None,
) -> call:
    return call.fetch_series_observations(
        series_id,
        observation_start=observation_start,
        observation_end=observation_end,
        realtime_start=realtime_start,
        realtime_end=realtime_end,
    )


def test_ingests_metadata_with_normalized_series_id() -> None:
    client = Mock(spec=FredClient)
    client.fetch_series.return_value = _metadata_payload("DFF")
    service = FredMacroeconomicIngestionService(client)

    result = service.ingest_metadata(" DFF ")

    client.fetch_series.assert_called_once_with("DFF")
    assert isinstance(result, FredSeriesMetadata)
    assert result.series_id == "DFF"


def test_ingests_observations_and_forwards_all_boundaries() -> None:
    client = Mock(spec=FredClient)
    client.fetch_series_observations.return_value = _observations_payload()
    service = FredMacroeconomicIngestionService(client)
    boundaries = {
        "observation_start": date(2026, 1, 1),
        "observation_end": date(2026, 8, 28),
        "realtime_start": date(2026, 8, 1),
        "realtime_end": date(2026, 8, 29),
    }

    result = service.ingest_observations(" DFF ", **boundaries)

    client.fetch_series_observations.assert_called_once_with(
        "DFF",
        **boundaries,
    )
    assert isinstance(result, FredSeriesObservations)
    assert result.series_id == "DFF"
    assert result.observations[0].series_id == "DFF"


def test_valid_empty_observations_remain_successful() -> None:
    client = Mock(spec=FredClient)
    client.fetch_series_observations.return_value = _observations_payload(empty=True)
    service = FredMacroeconomicIngestionService(client)

    result = service.ingest_observations("DFF")

    assert result.observations == ()


def test_complete_series_uses_exact_request_order_and_matching_identities() -> None:
    client = _valid_client()
    service = FredMacroeconomicIngestionService(client)

    result = service.ingest_series(" DFF ")

    assert client.method_calls == [
        call.fetch_series("DFF"),
        _observation_call("DFF"),
    ]
    assert isinstance(result, FredSeriesSourceData)
    assert result.series_id == "DFF"
    assert result.metadata.series_id == "DFF"
    assert result.observations.series_id == "DFF"


def test_metadata_provider_failure_prevents_observation_request() -> None:
    client = Mock(spec=FredClient)
    client.fetch_series.side_effect = FredError("provider failed")
    service = FredMacroeconomicIngestionService(client)

    with pytest.raises(FredError, match="provider failed"):
        service.ingest_series("DFF")

    client.fetch_series_observations.assert_not_called()


def test_metadata_parsing_failure_prevents_observation_request() -> None:
    client = Mock(spec=FredClient)
    client.fetch_series.return_value = {"seriess": []}
    service = FredMacroeconomicIngestionService(client)

    with pytest.raises(FredSeriesMetadataValidationError):
        service.ingest_series("DFF")

    client.fetch_series_observations.assert_not_called()


def test_observation_provider_failure_propagates() -> None:
    client = Mock(spec=FredClient)
    client.fetch_series.return_value = _metadata_payload("DFF")
    client.fetch_series_observations.side_effect = FredError("provider failed")
    service = FredMacroeconomicIngestionService(client)

    with pytest.raises(FredError, match="provider failed"):
        service.ingest_series("DFF")

    assert client.method_calls == [
        call.fetch_series("DFF"),
        _observation_call("DFF"),
    ]


def test_observation_parsing_failure_propagates() -> None:
    client = Mock(spec=FredClient)
    client.fetch_series.return_value = _metadata_payload("DFF")
    client.fetch_series_observations.return_value = {"observations": [{}]}
    service = FredMacroeconomicIngestionService(client)

    with pytest.raises(FredObservationValidationError):
        service.ingest_series("DFF")

    assert client.method_calls == [
        call.fetch_series("DFF"),
        _observation_call("DFF"),
    ]


@pytest.mark.parametrize(
    ("boundaries", "message"),
    [
        (
            {
                "observation_start": date(2026, 2, 1),
                "observation_end": date(2026, 1, 1),
            },
            "Observation start",
        ),
        (
            {
                "realtime_start": date(2026, 2, 1),
                "realtime_end": date(2026, 1, 1),
            },
            "Realtime start",
        ),
    ],
)
def test_rejects_reversed_batch_range_before_provider_calls(
    boundaries: dict[str, date],
    message: str,
) -> None:
    client = Mock(spec=FredClient)
    service = FredMacroeconomicIngestionService(client)

    with pytest.raises(ValueError, match=message):
        service.ingest_series_ids(["DFF"], **boundaries)

    assert client.method_calls == []


def test_rejects_whitespace_equivalent_duplicates_before_provider_calls() -> None:
    client = Mock(spec=FredClient)
    service = FredMacroeconomicIngestionService(client)

    with pytest.raises(ValueError, match="Duplicate series ID"):
        service.ingest_series_ids(["DFF", " DFF "])

    assert client.method_calls == []


def test_invalid_later_series_id_prevents_all_provider_calls() -> None:
    client = Mock(spec=FredClient)
    service = FredMacroeconomicIngestionService(client)

    with pytest.raises(ValueError, match="Series ID"):
        service.ingest_series_ids(["DFF", "   ", "UNRATE"])

    assert client.method_calls == []


def test_multiple_series_preserve_input_and_sequential_request_order() -> None:
    client = _valid_client()
    service = FredMacroeconomicIngestionService(client)

    result = service.ingest_series_ids([" DFF ", "CPIAUCSL"])

    assert list(result) == ["DFF", "CPIAUCSL"]
    assert client.method_calls == [
        call.fetch_series("DFF"),
        _observation_call("DFF"),
        call.fetch_series("CPIAUCSL"),
        _observation_call("CPIAUCSL"),
    ]


def test_empty_series_iterable_returns_empty_without_provider_calls() -> None:
    client = Mock(spec=FredClient)
    service = FredMacroeconomicIngestionService(client)

    assert service.ingest_series_ids([]) == {}
    assert client.method_calls == []


def test_middle_series_failure_prevents_later_series_requests() -> None:
    client = _valid_client()

    def fetch_series(series_id: str) -> dict[str, object]:
        if series_id == "CPIAUCSL":
            raise FredError("provider failed")
        return _metadata_payload(series_id)

    client.fetch_series.side_effect = fetch_series
    service = FredMacroeconomicIngestionService(client)

    with pytest.raises(FredError, match="provider failed"):
        service.ingest_series_ids(["DFF", "CPIAUCSL", "UNRATE"])

    assert client.method_calls == [
        call.fetch_series("DFF"),
        _observation_call("DFF"),
        call.fetch_series("CPIAUCSL"),
    ]


def test_initial_series_use_configured_order_and_forward_boundaries() -> None:
    client = _valid_client()
    service = FredMacroeconomicIngestionService(client)
    boundaries = {
        "observation_start": date(2020, 1, 1),
        "observation_end": date(2026, 8, 28),
        "realtime_start": date(2026, 8, 1),
        "realtime_end": date(2026, 8, 29),
    }

    result = service.ingest_initial_series(**boundaries)

    assert list(result) == list(INITIAL_FRED_SERIES_IDS)
    assert client.method_calls == [
        request
        for series_id in INITIAL_FRED_SERIES_IDS
        for request in (
            call.fetch_series(series_id),
            _observation_call(series_id, **boundaries),
        )
    ]
