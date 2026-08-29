import traceback
from unittest.mock import Mock

import pytest
import requests

from finstream.sec.edgar import SecEdgarClient, SecEdgarError


_HEADERS = {
    "User-Agent": "FinStream test@example.com",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
}


def _mock_session(payload: object) -> tuple[Mock, Mock]:
    response = Mock()
    response.json.return_value = payload
    session = Mock(spec=requests.Session)
    session.get.return_value = response
    return session, response


@pytest.mark.parametrize("user_agent", ["", "   ", None])
def test_rejects_blank_user_agent_before_request(user_agent: object) -> None:
    session = Mock(spec=requests.Session)

    with pytest.raises(ValueError, match="User-Agent"):
        SecEdgarClient(user_agent, session=session)  # type: ignore[arg-type]

    session.get.assert_not_called()


def test_strips_user_agent_and_forwards_headers_and_timeout() -> None:
    payload = {"cik": "0000320193"}
    session, _ = _mock_session(payload)
    client = SecEdgarClient(
        "  FinStream test@example.com  ",
        session=session,
        timeout_seconds=9.5,
    )

    result = client.fetch_submissions(320193)

    session.get.assert_called_once_with(
        "https://data.sec.gov/submissions/CIK0000320193.json",
        headers=_HEADERS,
        timeout=9.5,
    )
    assert result is payload


@pytest.mark.parametrize("timeout_seconds", [0, -1])
def test_rejects_non_positive_timeout(timeout_seconds: float) -> None:
    with pytest.raises(ValueError, match="Timeout"):
        SecEdgarClient("FinStream test@example.com", timeout_seconds=timeout_seconds)


@pytest.mark.parametrize("cik", [320193, "320193", "0000320193"])
def test_normalizes_cik_for_submissions_endpoint(cik: str | int) -> None:
    session, _ = _mock_session({})
    client = SecEdgarClient("FinStream test@example.com", session=session)

    client.fetch_submissions(cik)

    session.get.assert_called_once_with(
        "https://data.sec.gov/submissions/CIK0000320193.json",
        headers=_HEADERS,
        timeout=15,
    )


@pytest.mark.parametrize(
    "cik",
    [True, 0, -1, "", "   ", "ABC123", "12345678901", 1.5, None],
)
def test_rejects_invalid_cik_before_request(cik: object) -> None:
    session = Mock(spec=requests.Session)
    client = SecEdgarClient("FinStream test@example.com", session=session)

    with pytest.raises((TypeError, ValueError)):
        client.fetch_submissions(cik)  # type: ignore[arg-type]

    session.get.assert_not_called()


def test_fetches_company_facts_from_exact_endpoint() -> None:
    payload = {"facts": {}}
    session, _ = _mock_session(payload)
    client = SecEdgarClient("FinStream test@example.com", session=session)

    result = client.fetch_company_facts("320193")

    session.get.assert_called_once_with(
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
        headers=_HEADERS,
        timeout=15,
    )
    assert result is payload


def test_wraps_network_failure() -> None:
    session = Mock(spec=requests.Session)
    session.get.side_effect = requests.ConnectionError("network unavailable")
    client = SecEdgarClient("FinStream test@example.com", session=session)

    with pytest.raises(SecEdgarError, match="^SEC EDGAR request failed$"):
        client.fetch_submissions(320193)


def test_wraps_http_failure_without_exposing_exception_chain() -> None:
    marker = "raw-request-details"
    session, response = _mock_session({})
    response.raise_for_status.side_effect = requests.HTTPError(marker)
    client = SecEdgarClient("FinStream test@example.com", session=session)

    with pytest.raises(SecEdgarError) as error:
        client.fetch_company_facts(320193)

    rendered_exception = "".join(traceback.format_exception(error.value))
    assert str(error.value) == "SEC EDGAR request failed"
    assert marker not in rendered_exception
    assert error.value.__cause__ is None


def test_raises_for_invalid_json() -> None:
    session, response = _mock_session(None)
    response.json.side_effect = ValueError("invalid JSON")
    client = SecEdgarClient("FinStream test@example.com", session=session)

    with pytest.raises(SecEdgarError, match="invalid JSON"):
        client.fetch_submissions(320193)


@pytest.mark.parametrize("payload", [[], "unexpected", None])
def test_rejects_non_object_payload(payload: object) -> None:
    session, _ = _mock_session(payload)
    client = SecEdgarClient("FinStream test@example.com", session=session)

    with pytest.raises(SecEdgarError, match="unexpected payload"):
        client.fetch_company_facts(320193)
