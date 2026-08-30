"""Synchronous client for the FRED API."""

import math
from datetime import date
from typing import Any

import requests


_BASE_URL = "https://api.stlouisfed.org"
_SERIES_URL = f"{_BASE_URL}/fred/series"
_OBSERVATIONS_URL = f"{_BASE_URL}/fred/series/observations"


class FredError(Exception):
    """Raised when a FRED request or response cannot be handled."""


def _normalize_series_id(series_id: str) -> str:
    if not isinstance(series_id, str) or not series_id.strip():
        raise ValueError("Series ID must not be blank")
    return series_id.strip()


class FredClient:
    """Retrieve raw series and observation payloads from FRED."""

    def __init__(
        self,
        api_key: str,
        session: requests.Session | None = None,
        timeout_seconds: float = 15,
    ) -> None:
        if not isinstance(api_key, str) or not api_key.strip():
            raise ValueError("API key must not be blank")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("Timeout must be a positive finite number")

        self._api_key = api_key.strip()
        self._session = session if session is not None else requests.Session()
        self._timeout_seconds = timeout_seconds

    def fetch_series(self, series_id: str) -> dict[str, Any]:
        """Return an unparsed FRED series metadata response."""
        return self._request(
            _SERIES_URL,
            params={"series_id": _normalize_series_id(series_id)},
            collection_key="seriess",
        )

    def fetch_series_observations(
        self,
        series_id: str,
        *,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> dict[str, Any]:
        """Return unparsed observations with optional source date boundaries."""
        if (
            observation_start is not None
            and observation_end is not None
            and observation_start > observation_end
        ):
            raise ValueError("Observation start must not be after observation end")
        if (
            realtime_start is not None
            and realtime_end is not None
            and realtime_start > realtime_end
        ):
            raise ValueError("Realtime start must not be after realtime end")

        params = {"series_id": _normalize_series_id(series_id)}
        optional_dates = {
            "observation_start": observation_start,
            "observation_end": observation_end,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
        }
        params.update(
            {
                name: value.isoformat()
                for name, value in optional_dates.items()
                if value is not None
            }
        )
        return self._request(
            _OBSERVATIONS_URL,
            params=params,
            collection_key="observations",
        )

    def _request(
        self,
        url: str,
        *,
        params: dict[str, str],
        collection_key: str,
    ) -> dict[str, Any]:
        request_params = {
            **params,
            "api_key": self._api_key,
            "file_type": "json",
        }
        try:
            response = self._session.get(
                url,
                params=request_params,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException:
            raise FredError("FRED request failed") from None

        try:
            payload = response.json()
        except ValueError:
            raise FredError("FRED returned invalid JSON") from None

        if not isinstance(payload, dict):
            raise FredError("FRED returned an unexpected payload")
        if "error_code" in payload or "error_message" in payload:
            raise FredError("FRED provider returned an error")
        if not isinstance(payload.get(collection_key), list):
            raise FredError(
                f"FRED response must contain a '{collection_key}' list"
            )

        return payload
