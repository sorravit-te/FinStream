"""Synchronous client for the SEC EDGAR data APIs."""

from typing import Any

import requests


_BASE_URL = "https://data.sec.gov"


class SecEdgarError(Exception):
    """Raised when an SEC EDGAR request or response cannot be handled."""


def _normalize_cik(cik: str | int) -> str:
    if isinstance(cik, bool):
        raise ValueError("CIK must be a positive integer or numeric string")

    if isinstance(cik, int):
        if cik <= 0:
            raise ValueError("CIK must be greater than zero")
        cik_value = str(cik)
    elif isinstance(cik, str):
        if not cik:
            raise ValueError("CIK must not be blank")
        if not cik.isdigit():
            raise ValueError("CIK must contain only digits")
        if int(cik) == 0:
            raise ValueError("CIK must be greater than zero")
        cik_value = cik
    else:
        raise TypeError("CIK must be a string or integer")

    if len(cik_value) > 10:
        raise ValueError("CIK must not exceed 10 digits")
    return cik_value.zfill(10)


class SecEdgarClient:
    """Retrieve raw JSON objects from SEC EDGAR data endpoints."""

    def __init__(
        self,
        user_agent: str,
        session: requests.Session | None = None,
        timeout_seconds: float = 15,
    ) -> None:
        if not isinstance(user_agent, str) or not user_agent.strip():
            raise ValueError("User-Agent must not be blank")
        if timeout_seconds <= 0:
            raise ValueError("Timeout must be greater than zero")

        self._headers = {
            "User-Agent": user_agent.strip(),
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }
        self._session = session if session is not None else requests.Session()
        self._timeout_seconds = timeout_seconds

    def fetch_submissions(self, cik: str | int) -> dict[str, Any]:
        """Return an unparsed SEC submissions response for one CIK."""
        normalized_cik = _normalize_cik(cik)
        return self._request(f"/submissions/CIK{normalized_cik}.json")

    def fetch_company_facts(self, cik: str | int) -> dict[str, Any]:
        """Return an unparsed SEC company-facts response for one CIK."""
        normalized_cik = _normalize_cik(cik)
        return self._request(f"/api/xbrl/companyfacts/CIK{normalized_cik}.json")

    def _request(self, path: str) -> dict[str, Any]:
        try:
            response = self._session.get(
                f"{_BASE_URL}{path}",
                headers=self._headers,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException:
            raise SecEdgarError("SEC EDGAR request failed") from None

        try:
            payload = response.json()
        except ValueError as exc:
            raise SecEdgarError("SEC EDGAR returned invalid JSON") from exc

        if not isinstance(payload, dict):
            raise SecEdgarError("SEC EDGAR returned an unexpected payload")
        return payload
