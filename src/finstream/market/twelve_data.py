"""Synchronous client for the Twelve Data REST API."""

from datetime import date
from typing import Any

import requests


_BASE_URL = "https://api.twelvedata.com"
_TIME_SERIES_URL = f"{_BASE_URL}/time_series"


class TwelveDataError(Exception):
    """Raised when a Twelve Data request or response cannot be handled."""


class TwelveDataClient:
    """Retrieve raw daily time-series payloads from Twelve Data."""

    def __init__(
        self,
        api_key: str,
        session: requests.Session | None = None,
        timeout_seconds: float = 15,
    ) -> None:
        if not isinstance(api_key, str) or not api_key.strip():
            raise ValueError("API key must not be blank")
        if timeout_seconds <= 0:
            raise ValueError("Timeout must be greater than zero")

        self._api_key = api_key.strip()
        self._session = session if session is not None else requests.Session()
        self._timeout_seconds = timeout_seconds

    def fetch_daily_time_series(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Return an unparsed daily time-series response for one symbol."""
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Symbol must not be blank")
        if (start_date is None) != (end_date is None):
            raise ValueError("Start date and end date must be supplied together")
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("Start date must not be after end date")

        params = {
            "symbol": symbol.strip().upper(),
            "interval": "1day",
            "apikey": self._api_key,
        }
        if start_date is not None and end_date is not None:
            params["start_date"] = start_date.isoformat()
            params["end_date"] = end_date.isoformat()

        try:
            response = self._session.get(
                _TIME_SERIES_URL,
                params=params,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException:
            raise TwelveDataError("Twelve Data request failed") from None

        try:
            payload = response.json()
        except ValueError as exc:
            raise TwelveDataError("Twelve Data returned invalid JSON") from exc

        if not isinstance(payload, dict):
            raise TwelveDataError("Twelve Data returned an unexpected payload")

        if payload.get("status") == "error":
            provider_message = payload.get("message")
            if isinstance(provider_message, str) and provider_message.strip():
                safe_message = provider_message.replace(self._api_key, "[REDACTED]")
                raise TwelveDataError(f"Twelve Data provider error: {safe_message}")
            raise TwelveDataError("Twelve Data provider returned an error")

        return payload
