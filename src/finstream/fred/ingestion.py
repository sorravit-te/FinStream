"""Application service for retrieving and parsing FRED source data."""

from collections.abc import Iterable
from datetime import date

from finstream.fred.client import FredClient
from finstream.fred.models import (
    FredSeriesMetadata,
    FredSeriesObservations,
    FredSeriesSourceData,
)
from finstream.fred.parsing import (
    parse_series_metadata,
    parse_series_observations,
)
from finstream.fred.series import INITIAL_FRED_SERIES_IDS


def _normalize_series_id(series_id: str) -> str:
    if not isinstance(series_id, str) or not series_id.strip():
        raise ValueError("Series ID must not be blank")
    return series_id.strip()


def _validate_ranges(
    *,
    observation_start: date | None,
    observation_end: date | None,
    realtime_start: date | None,
    realtime_end: date | None,
) -> None:
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


class FredMacroeconomicIngestionService:
    """Coordinate FRED retrieval and source-aligned parsing."""

    def __init__(self, client: FredClient) -> None:
        self._client = client

    def ingest_metadata(self, series_id: str) -> FredSeriesMetadata:
        """Retrieve and parse metadata for one FRED series."""
        return self._ingest_metadata(_normalize_series_id(series_id))

    def ingest_observations(
        self,
        series_id: str,
        *,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> FredSeriesObservations:
        """Retrieve and parse observations for one FRED series."""
        _validate_ranges(
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        return self._ingest_observations(
            _normalize_series_id(series_id),
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )

    def ingest_series(
        self,
        series_id: str,
        *,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> FredSeriesSourceData:
        """Retrieve complete source data for one FRED series."""
        _validate_ranges(
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        return self._ingest_series(
            _normalize_series_id(series_id),
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )

    def ingest_series_ids(
        self,
        series_ids: Iterable[str],
        *,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> dict[str, FredSeriesSourceData]:
        """Validate, then ingest FRED series sequentially in input order."""
        _validate_ranges(
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )

        normalized_ids: list[str] = []
        seen_ids: set[str] = set()
        for series_id in series_ids:
            normalized_id = _normalize_series_id(series_id)
            if normalized_id in seen_ids:
                raise ValueError(f"Duplicate series ID: {normalized_id}")
            normalized_ids.append(normalized_id)
            seen_ids.add(normalized_id)

        results: dict[str, FredSeriesSourceData] = {}
        for normalized_id in normalized_ids:
            results[normalized_id] = self._ingest_series(
                normalized_id,
                observation_start=observation_start,
                observation_end=observation_end,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
            )
        return results

    def ingest_initial_series(
        self,
        *,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> dict[str, FredSeriesSourceData]:
        """Ingest the configured initial FinStream FRED series."""
        return self.ingest_series_ids(
            INITIAL_FRED_SERIES_IDS,
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )

    def _ingest_metadata(self, normalized_id: str) -> FredSeriesMetadata:
        payload = self._client.fetch_series(normalized_id)
        return parse_series_metadata(payload, expected_series_id=normalized_id)

    def _ingest_observations(
        self,
        normalized_id: str,
        *,
        observation_start: date | None,
        observation_end: date | None,
        realtime_start: date | None,
        realtime_end: date | None,
    ) -> FredSeriesObservations:
        payload = self._client.fetch_series_observations(
            normalized_id,
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        return parse_series_observations(
            payload,
            expected_series_id=normalized_id,
        )

    def _ingest_series(
        self,
        normalized_id: str,
        *,
        observation_start: date | None,
        observation_end: date | None,
        realtime_start: date | None,
        realtime_end: date | None,
    ) -> FredSeriesSourceData:
        metadata = self._ingest_metadata(normalized_id)
        observations = self._ingest_observations(
            normalized_id,
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        return FredSeriesSourceData(
            series_id=normalized_id,
            metadata=metadata,
            observations=observations,
        )
