"""Application service for retrieving and parsing FRED source data."""

from collections.abc import Iterable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg

from finstream.bronze.json_storage import write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import write_parquet
from finstream.bronze.paths import DEFAULT_BRONZE_ROOT
from finstream.bronze.recovery import recover_or_verify_bronze_artifacts
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
    FredBronzeDatasetResult,
    FredSeriesBronzeResult,
    fred_series_metadata_to_table,
    fred_series_observations_to_table,
)
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
from finstream.fred.postgres import latest_fred_observation_date
from finstream.fred.series import INITIAL_FRED_SERIES_IDS


FRED_INCREMENTAL_OVERLAP_DAYS = 365


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


def fred_incremental_observation_start_date(
    latest_observation_date: date | None,
    *,
    overlap_days: int = FRED_INCREMENTAL_OVERLAP_DAYS,
) -> date | None:
    """Return a calendar-day lower bound for incremental FRED observations."""
    if latest_observation_date is None:
        return None
    if isinstance(latest_observation_date, datetime) or not isinstance(
        latest_observation_date, date
    ):
        raise ValueError("Latest observation date must be a date or None")
    if isinstance(overlap_days, bool) or not isinstance(overlap_days, int):
        raise ValueError("FRED incremental overlap days must be an integer")
    if overlap_days < 0:
        raise ValueError("FRED incremental overlap days must not be negative")
    return latest_observation_date - timedelta(days=overlap_days)


class FredMacroeconomicIngestionService:
    """Coordinate FRED retrieval and source-aligned parsing."""

    def __init__(self, client: FredClient) -> None:
        self._client = client

    def ingest_metadata(self, series_id: str) -> FredSeriesMetadata:
        """Retrieve and parse metadata for one FRED series."""
        return self._ingest_metadata(_normalize_series_id(series_id))

    def ingest_metadata_to_bronze(
        self,
        series_id: str,
        *,
        run_at: datetime,
        bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
    ) -> FredBronzeDatasetResult:
        """Retrieve and persist one FRED series metadata Bronze dataset."""
        normalized_id = _normalize_series_id(series_id)
        location = self._bronze_location(
            dataset=FRED_SERIES_METADATA_BRONZE_DATASET,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        recovered = self._recover_metadata_bronze(normalized_id, location)
        if recovered is not None:
            return recovered
        return self._ingest_metadata_to_bronze(normalized_id, location)

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

    def ingest_observations_to_bronze(
        self,
        series_id: str,
        *,
        run_at: datetime,
        bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> FredBronzeDatasetResult:
        """Retrieve and persist one FRED observations Bronze dataset."""
        normalized_id = _normalize_series_id(series_id)
        _validate_ranges(
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        location = self._bronze_location(
            dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        recovered = self._recover_observations_bronze(normalized_id, location)
        if recovered is not None:
            return recovered
        return self._ingest_observations_to_bronze(
            normalized_id,
            location,
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

    def ingest_series_to_bronze(
        self,
        series_id: str,
        *,
        run_at: datetime,
        bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> FredSeriesBronzeResult:
        """Retrieve and persist both FRED Bronze datasets for one series."""
        normalized_id = _normalize_series_id(series_id)
        _validate_ranges(
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        metadata_location = self._bronze_location(
            dataset=FRED_SERIES_METADATA_BRONZE_DATASET,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        observations_location = self._bronze_location(
            dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        metadata = self._recover_metadata_bronze(normalized_id, metadata_location)
        observations = self._recover_observations_bronze(
            normalized_id,
            observations_location,
        )
        if metadata is None:
            metadata = self._ingest_metadata_to_bronze(normalized_id, metadata_location)
        if observations is None:
            observations = self._ingest_observations_to_bronze(
                normalized_id,
                observations_location,
                observation_start=observation_start,
                observation_end=observation_end,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
            )
        return FredSeriesBronzeResult(
            series_id=normalized_id,
            metadata=metadata,
            observations=observations,
        )

    def ingest_series_incrementally_to_bronze(
        self,
        connection: psycopg.Connection,
        series_id: str,
        *,
        run_at: datetime,
        bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
        observation_start: date | None = None,
        observation_end: date | None = None,
        realtime_start: date | None = None,
        realtime_end: date | None = None,
    ) -> FredSeriesBronzeResult:
        """Create a full-metadata and bounded-observations FRED Bronze run."""
        normalized_id = _normalize_series_id(series_id)
        _validate_ranges(
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )

        if observation_start is None and observation_end is None:
            observation_start = fred_incremental_observation_start_date(
                latest_fred_observation_date(connection, normalized_id)
            )

        return self.ingest_series_to_bronze(
            normalized_id,
            run_at=run_at,
            bronze_root=bronze_root,
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

    @staticmethod
    def _bronze_location(
        *,
        dataset: str,
        run_at: datetime,
        bronze_root: str | Path,
    ) -> BronzeRunLocation:
        return BronzeRunLocation.from_run(
            root=bronze_root,
            source=FRED_BRONZE_SOURCE,
            dataset=dataset,
            ingested_at=run_at,
        )

    @staticmethod
    def _recovered_dataset_result(
        series_id: str,
        location: BronzeRunLocation,
        record_count: int,
    ) -> FredBronzeDatasetResult:
        return FredBronzeDatasetResult(
            series_id=series_id,
            location=location,
            raw_json_path=location.directory / "payload.json",
            parquet_path=location.directory / "data.parquet",
            record_count=record_count,
        )

    def _recover_metadata_bronze(
        self,
        normalized_id: str,
        location: BronzeRunLocation,
    ) -> FredBronzeDatasetResult | None:
        recovered = recover_or_verify_bronze_artifacts(
            location,
            table_from_payload=lambda payload: fred_series_metadata_to_table(
                parse_series_metadata(payload, expected_series_id=normalized_id)
            ),
        )
        if recovered is None:
            return None
        return self._recovered_dataset_result(normalized_id, location, 1)

    def _recover_observations_bronze(
        self,
        normalized_id: str,
        location: BronzeRunLocation,
    ) -> FredBronzeDatasetResult | None:
        recovered = recover_or_verify_bronze_artifacts(
            location,
            table_from_payload=lambda payload: fred_series_observations_to_table(
                parse_series_observations(payload, expected_series_id=normalized_id)
            ),
        )
        if recovered is None:
            return None
        return self._recovered_dataset_result(
            normalized_id,
            location,
            recovered.table.num_rows,
        )

    def _ingest_metadata_to_bronze(
        self,
        normalized_id: str,
        location: BronzeRunLocation,
    ) -> FredBronzeDatasetResult:
        payload, metadata = self._fetch_and_parse_metadata(normalized_id)
        table = fred_series_metadata_to_table(metadata)
        persisted_raw_json_path = write_raw_json(location, payload)
        persisted_parquet_path = write_parquet(location, table)
        return FredBronzeDatasetResult(
            series_id=normalized_id,
            location=location,
            raw_json_path=persisted_raw_json_path,
            parquet_path=persisted_parquet_path,
            record_count=1,
        )

    def _ingest_observations_to_bronze(
        self,
        normalized_id: str,
        location: BronzeRunLocation,
        *,
        observation_start: date | None,
        observation_end: date | None,
        realtime_start: date | None,
        realtime_end: date | None,
    ) -> FredBronzeDatasetResult:
        payload, observations = self._fetch_and_parse_observations(
            normalized_id,
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        table = fred_series_observations_to_table(observations)
        persisted_raw_json_path = write_raw_json(location, payload)
        persisted_parquet_path = write_parquet(location, table)
        return FredBronzeDatasetResult(
            series_id=normalized_id,
            location=location,
            raw_json_path=persisted_raw_json_path,
            parquet_path=persisted_parquet_path,
            record_count=len(observations.observations),
        )

    def _fetch_and_parse_metadata(
        self,
        normalized_id: str,
    ) -> tuple[dict[str, Any], FredSeriesMetadata]:
        payload = self._client.fetch_series(normalized_id)
        metadata = parse_series_metadata(payload, expected_series_id=normalized_id)
        return payload, metadata

    def _ingest_metadata(self, normalized_id: str) -> FredSeriesMetadata:
        _, metadata = self._fetch_and_parse_metadata(normalized_id)
        return metadata

    def _fetch_and_parse_observations(
        self,
        normalized_id: str,
        *,
        observation_start: date | None,
        observation_end: date | None,
        realtime_start: date | None,
        realtime_end: date | None,
    ) -> tuple[dict[str, Any], FredSeriesObservations]:
        payload = self._client.fetch_series_observations(
            normalized_id,
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        observations = parse_series_observations(
            payload,
            expected_series_id=normalized_id,
        )
        return payload, observations

    def _ingest_observations(
        self,
        normalized_id: str,
        *,
        observation_start: date | None,
        observation_end: date | None,
        realtime_start: date | None,
        realtime_end: date | None,
    ) -> FredSeriesObservations:
        _, observations = self._fetch_and_parse_observations(
            normalized_id,
            observation_start=observation_start,
            observation_end=observation_end,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
        )
        return observations

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
