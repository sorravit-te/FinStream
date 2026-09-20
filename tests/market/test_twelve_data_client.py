from datetime import date
import traceback
from unittest.mock import Mock

import pytest
import requests

from finstream.market.twelve_data import TwelveDataClient, TwelveDataError


def _mock_session(payload: object) -> tuple[Mock, Mock]:
    response = Mock()
    response.json.return_value = payload
    session = Mock(spec=requests.Session)
    session.get.return_value = response
    return session, response


def test_constructs_daily_time_series_request() -> None:
    payload = {"status": "ok", "values": []}
    session, _ = _mock_session(payload)
    client = TwelveDataClient("test-key", session=session, timeout_seconds=9.5)

    result = client.fetch_daily_time_series(
        " aapl ",
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 31),
    )

    session.get.assert_called_once_with(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": "AAPL",
            "interval": "1day",
            "apikey": "test-key",
            "start_date": "2026-01-02",
            "end_date": "2026-01-31",
        },
        timeout=9.5,
    )
    assert result is payload


def test_constructs_lower_bound_only_daily_time_series_request() -> None:
    payload = {"status": "ok", "values": []}
    session, _ = _mock_session(payload)
    client = TwelveDataClient("test-key", session=session, timeout_seconds=9.5)

    result = client.fetch_daily_time_series_since(
        " aapl ",
        start_date=date(2026, 9, 15),
    )

    session.get.assert_called_once_with(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": "AAPL",
            "interval": "1day",
            "apikey": "test-key",
            "start_date": "2026-09-15",
        },
        timeout=9.5,
    )
    assert result is payload


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    [
        (date(2026, 1, 1), None),
        (None, date(2026, 1, 31)),
    ],
)
def test_rejects_partial_date_range(
    start_date: date | None,
    end_date: date | None,
) -> None:
    client = TwelveDataClient("test-key", session=Mock(spec=requests.Session))

    with pytest.raises(ValueError, match="supplied together"):
        client.fetch_daily_time_series("AAPL", start_date, end_date)


def test_rejects_reversed_date_range() -> None:
    client = TwelveDataClient("test-key", session=Mock(spec=requests.Session))

    with pytest.raises(ValueError, match="must not be after"):
        client.fetch_daily_time_series(
            "AAPL",
            start_date=date(2026, 2, 1),
            end_date=date(2026, 1, 1),
        )


@pytest.mark.parametrize("api_key", ["", "   "])
def test_rejects_blank_api_key(api_key: str) -> None:
    with pytest.raises(ValueError, match="API key"):
        TwelveDataClient(api_key)


def test_rejects_blank_symbol() -> None:
    client = TwelveDataClient("test-key", session=Mock(spec=requests.Session))

    with pytest.raises(ValueError, match="Symbol"):
        client.fetch_daily_time_series("   ")


@pytest.mark.parametrize("timeout_seconds", [0, -1])
def test_rejects_non_positive_timeout(timeout_seconds: float) -> None:
    with pytest.raises(ValueError, match="Timeout"):
        TwelveDataClient("test-key", timeout_seconds=timeout_seconds)


def test_wraps_http_error_without_exposing_api_key() -> None:
    api_key = "secret-key"
    session, response = _mock_session({"status": "ok"})
    response.raise_for_status.side_effect = requests.HTTPError(
        f"https://api.twelvedata.com/time_series?apikey={api_key}"
    )
    client = TwelveDataClient(api_key, session=session)

    with pytest.raises(TwelveDataError) as error:
        client.fetch_daily_time_series("AAPL")

    rendered_exception = "".join(traceback.format_exception(error.value))
    assert api_key not in str(error.value)
    assert api_key not in rendered_exception
    assert error.value.__cause__ is None


def test_raises_for_provider_error_payload() -> None:
    session, _ = _mock_session(
        {"status": "error", "message": "Invalid symbol"}
    )
    client = TwelveDataClient("test-key", session=session)

    with pytest.raises(TwelveDataError, match="Invalid symbol"):
        client.fetch_daily_time_series("INVALID")


def test_raises_for_invalid_json() -> None:
    session, response = _mock_session(None)
    response.json.side_effect = ValueError("invalid JSON")
    client = TwelveDataClient("test-key", session=session)

    with pytest.raises(TwelveDataError, match="invalid JSON"):
        client.fetch_daily_time_series("AAPL")


def test_rejects_non_object_payload() -> None:
    session, _ = _mock_session([{"status": "ok"}])
    client = TwelveDataClient("test-key", session=session)

    with pytest.raises(TwelveDataError, match="unexpected payload"):
        client.fetch_daily_time_series("AAPL")
