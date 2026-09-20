from datetime import date
import traceback
from unittest.mock import Mock

import pytest
import requests

from finstream.fred.client import FredClient, FredError


def _mock_session(payload: object) -> tuple[Mock, Mock]:
    response = Mock()
    response.json.return_value = payload
    session = Mock(spec=requests.Session)
    session.get.return_value = response
    return session, response


def test_fetches_series_metadata_from_canonical_endpoint() -> None:
    payload = {"seriess": [{"id": "DFF"}]}
    session, _ = _mock_session(payload)
    client = FredClient("test-key", session=session, timeout_seconds=9.5)

    result = client.fetch_series(" DFF ")

    session.get.assert_called_once_with(
        "https://api.stlouisfed.org/fred/series",
        params={
            "series_id": "DFF",
            "api_key": "test-key",
            "file_type": "json",
        },
        timeout=9.5,
    )
    assert result is payload


def test_fetches_observations_with_explicit_date_parameters() -> None:
    payload = {"observations": []}
    session, _ = _mock_session(payload)
    client = FredClient("test-key", session=session)

    result = client.fetch_series_observations(
        "CPIAUCSL",
        observation_start=date(2025, 1, 1),
        observation_end=date(2025, 12, 31),
        realtime_start=date(2026, 1, 1),
        realtime_end=date(2026, 1, 31),
    )

    session.get.assert_called_once_with(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": "CPIAUCSL",
            "observation_start": "2025-01-01",
            "observation_end": "2025-12-31",
            "realtime_start": "2026-01-01",
            "realtime_end": "2026-01-31",
            "api_key": "test-key",
            "file_type": "json",
        },
        timeout=15,
    )
    assert result is payload


def test_omits_unsupplied_observation_parameters() -> None:
    session, _ = _mock_session({"observations": []})
    client = FredClient("test-key", session=session)

    client.fetch_series_observations("DFF")

    assert session.get.call_args.kwargs["params"] == {
        "series_id": "DFF",
        "api_key": "test-key",
        "file_type": "json",
    }


@pytest.mark.parametrize("api_key", ["", "   ", None])
def test_rejects_blank_api_key(api_key: object) -> None:
    with pytest.raises(ValueError, match="API key"):
        FredClient(api_key)  # type: ignore[arg-type]


@pytest.mark.parametrize("series_id", ["", "   ", None])
def test_rejects_blank_series_id_before_request(series_id: object) -> None:
    session = Mock(spec=requests.Session)
    client = FredClient("test-key", session=session)

    with pytest.raises(ValueError, match="Series ID"):
        client.fetch_series(series_id)  # type: ignore[arg-type]

    session.get.assert_not_called()


@pytest.mark.parametrize(
    "timeout_seconds",
    [0, -1, float("nan"), float("inf"), True, "15"],
)
def test_rejects_invalid_timeout(timeout_seconds: object) -> None:
    with pytest.raises(ValueError, match="Timeout"):
        FredClient("test-key", timeout_seconds=timeout_seconds)  # type: ignore[arg-type]


def test_rejects_reversed_observation_range_before_request() -> None:
    session = Mock(spec=requests.Session)
    client = FredClient("test-key", session=session)

    with pytest.raises(ValueError, match="Observation start"):
        client.fetch_series_observations(
            "DFF",
            observation_start=date(2026, 2, 1),
            observation_end=date(2026, 1, 1),
        )

    session.get.assert_not_called()


def test_rejects_reversed_realtime_range_before_request() -> None:
    session = Mock(spec=requests.Session)
    client = FredClient("test-key", session=session)

    with pytest.raises(ValueError, match="Realtime start"):
        client.fetch_series_observations(
            "DFF",
            realtime_start=date(2026, 2, 1),
            realtime_end=date(2026, 1, 1),
        )

    session.get.assert_not_called()


def test_wraps_http_failure_without_exposing_api_key() -> None:
    api_key = "secret-test-key"
    session, response = _mock_session({})
    response.raise_for_status.side_effect = requests.HTTPError(
        f"https://api.stlouisfed.org/fred/series?api_key={api_key}"
    )
    client = FredClient(api_key, session=session)

    with pytest.raises(FredError) as error:
        client.fetch_series("DFF")

    rendered_exception = "".join(traceback.format_exception(error.value))
    assert str(error.value) == "FRED request failed"
    assert api_key not in rendered_exception
    assert error.value.__cause__ is None


def test_wraps_invalid_json_without_exposing_api_key() -> None:
    api_key = "secret-test-key"
    session, response = _mock_session(None)
    response.json.side_effect = ValueError(f"response URL contained {api_key}")
    client = FredClient(api_key, session=session)

    with pytest.raises(FredError) as error:
        client.fetch_series("DFF")

    rendered_exception = "".join(traceback.format_exception(error.value))
    assert str(error.value) == "FRED returned invalid JSON"
    assert api_key not in rendered_exception
    assert error.value.__cause__ is None


@pytest.mark.parametrize("payload", [[], "unexpected", None])
def test_rejects_non_object_payload(payload: object) -> None:
    session, _ = _mock_session(payload)
    client = FredClient("test-key", session=session)

    with pytest.raises(FredError, match="unexpected payload"):
        client.fetch_series("DFF")


def test_rejects_provider_error_payload_without_exposing_message() -> None:
    session, _ = _mock_session(
        {"error_code": 400, "error_message": "provider request details"}
    )
    client = FredClient("test-key", session=session)

    with pytest.raises(FredError, match="provider returned an error") as error:
        client.fetch_series("DFF")

    assert "provider request details" not in str(error.value)


def test_rejects_metadata_response_missing_seriess() -> None:
    session, _ = _mock_session({})
    client = FredClient("test-key", session=session)

    with pytest.raises(FredError, match="'seriess' list"):
        client.fetch_series("DFF")


def test_rejects_metadata_response_with_non_list_seriess() -> None:
    session, _ = _mock_session({"seriess": {}})
    client = FredClient("test-key", session=session)

    with pytest.raises(FredError, match="'seriess' list"):
        client.fetch_series("DFF")


def test_accepts_metadata_response_with_empty_seriess() -> None:
    payload = {"seriess": []}
    session, _ = _mock_session(payload)
    client = FredClient("test-key", session=session)

    assert client.fetch_series("DFF") is payload


def test_rejects_observations_response_missing_observations() -> None:
    session, _ = _mock_session({})
    client = FredClient("test-key", session=session)

    with pytest.raises(FredError, match="'observations' list"):
        client.fetch_series_observations("DFF")


def test_rejects_observations_response_with_non_list_observations() -> None:
    session, _ = _mock_session({"observations": {}})
    client = FredClient("test-key", session=session)

    with pytest.raises(FredError, match="'observations' list"):
        client.fetch_series_observations("DFF")


def test_accepts_observations_response_with_empty_observations() -> None:
    payload = {"observations": []}
    session, _ = _mock_session(payload)
    client = FredClient("test-key", session=session)

    assert client.fetch_series_observations("DFF") is payload
