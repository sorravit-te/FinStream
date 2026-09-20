"""Load validated FRED Bronze Parquet runs into PostgreSQL source tables."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

import psycopg
import pyarrow as pa

from finstream.bronze.json_storage import raw_json_path
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import parquet_path, read_parquet
from finstream.database.replay import (
    IngestionRun,
    IngestionRunReplayError,
    IngestionRunState,
    inspect_ingestion_run,
    register_or_verify_ingestion_run,
)
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_METADATA_SCHEMA,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_SCHEMA,
    FredBronzeDatasetResult,
    FredSeriesBronzeResult,
)


_INSERT_INGESTION_RUN = """
    INSERT INTO source_data.ingestion_runs (
        source,
        dataset,
        run_id,
        ingested_at,
        raw_json_path,
        parquet_path,
        record_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source, dataset, run_id) DO NOTHING
    RETURNING 1
"""

_INSERT_FRED_METADATA = """
    INSERT INTO source_data.fred_series_metadata (
        source,
        dataset,
        run_id,
        source_row_number,
        series_id,
        realtime_start,
        realtime_end,
        title,
        observation_start,
        observation_end,
        frequency,
        frequency_short,
        units,
        units_short,
        seasonal_adjustment,
        seasonal_adjustment_short,
        last_updated,
        popularity,
        notes
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_INSERT_FRED_OBSERVATION = """
    INSERT INTO source_data.fred_series_observations (
        source,
        dataset,
        run_id,
        source_row_number,
        series_id,
        realtime_start,
        realtime_end,
        observation_date,
        value
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_SELECT_LATEST_FRED_OBSERVATION_DATE = """
    SELECT max(observation_date)
    FROM source_data.fred_series_observations
    WHERE source = %s AND dataset = %s AND series_id = %s
"""


class FredPostgresLoadError(ValueError):
    """Raised when FRED Bronze data violates the loading contract."""


def latest_fred_observation_date(
    connection: psycopg.Connection,
    series_id: str,
) -> date | None:
    """Return the latest source-aligned observation date for one FRED series."""
    if not isinstance(series_id, str) or not series_id.strip():
        raise FredPostgresLoadError("FRED series ID must not be blank")
    normalized_series_id = series_id.strip()

    with connection.cursor() as cursor:
        cursor.execute(
            _SELECT_LATEST_FRED_OBSERVATION_DATE,
            (
                FRED_BRONZE_SOURCE,
                FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
                normalized_series_id,
            ),
        )
        row = cursor.fetchone()

    if row is None:
        raise FredPostgresLoadError(
            "FRED observation-date watermark query returned no row"
        )
    latest_observation_date = row[0]
    if latest_observation_date is not None and not isinstance(
        latest_observation_date, date
    ):
        raise FredPostgresLoadError(
            "FRED observation-date watermark query returned an invalid date"
        )
    return latest_observation_date


@dataclass(frozen=True)
class FredPostgresLoadResult:
    """Counts produced by loading a complete FRED series Bronze run."""

    metadata_loaded: int
    observations_loaded: int


@dataclass(frozen=True)
class _PreparedFredDataset:
    result: FredBronzeDatasetResult
    table: pa.Table


def _validate_series_id(series_id: object) -> str:
    if (
        not isinstance(series_id, str)
        or not series_id.strip()
        or series_id != series_id.strip()
    ):
        raise FredPostgresLoadError(
            "FRED series ID must be a non-blank normalized string"
        )
    return series_id


def _preflight_dataset(
    result: object,
    *,
    expected_dataset: str,
    expected_schema: pa.Schema,
    require_exactly_one_row: bool = False,
) -> _PreparedFredDataset:
    """Validate one FRED Bronze dataset completely before any SQL is issued."""
    if type(result) is not FredBronzeDatasetResult:
        raise FredPostgresLoadError("result must be a FredBronzeDatasetResult")
    if not isinstance(result.location, BronzeRunLocation):
        raise FredPostgresLoadError("FRED Bronze location must be a BronzeRunLocation")

    series_id = _validate_series_id(result.series_id)
    metadata = result.location.metadata
    if metadata.source != FRED_BRONZE_SOURCE:
        raise FredPostgresLoadError("FRED Bronze source must be fred")
    if metadata.dataset != expected_dataset:
        raise FredPostgresLoadError("FRED Bronze dataset does not match this loader")
    if metadata.entity is not None and metadata.entity != series_id:
        raise FredPostgresLoadError("FRED Bronze entity does not match series ID")
    if result.raw_json_path != raw_json_path(result.location):
        raise FredPostgresLoadError("FRED Bronze raw JSON path is not canonical")
    if result.parquet_path != parquet_path(result.location):
        raise FredPostgresLoadError("FRED Bronze Parquet path is not canonical")
    if isinstance(result.record_count, bool) or not isinstance(result.record_count, int):
        raise FredPostgresLoadError("FRED Bronze record count must be an integer")
    if result.record_count < 0:
        raise FredPostgresLoadError("FRED Bronze record count must not be negative")

    table = read_parquet(result.location)
    if not table.schema.equals(expected_schema, check_metadata=True):
        raise FredPostgresLoadError("FRED Bronze Parquet schema does not match")
    try:
        table.validate(full=True)
    except pa.ArrowException as exc:
        raise FredPostgresLoadError(
            "FRED Bronze Parquet table failed Arrow validation"
        ) from exc
    if table.num_rows != result.record_count:
        raise FredPostgresLoadError(
            "FRED Bronze record count does not match Parquet row count"
        )
    if require_exactly_one_row and (result.record_count != 1 or table.num_rows != 1):
        raise FredPostgresLoadError("FRED metadata Bronze must contain exactly one row")
    if any(row_series_id != series_id for row_series_id in table.column("series_id").to_pylist()):
        raise FredPostgresLoadError(
            "FRED Bronze Parquet series IDs do not match the result"
        )
    return _PreparedFredDataset(result=result, table=table)


def _ingestion_run(
    prepared: _PreparedFredDataset,
    *,
    source_table: str,
) -> IngestionRun:
    result = prepared.result
    metadata = result.location.metadata
    return IngestionRun(
        source=metadata.source,
        dataset=metadata.dataset,
        run_id=metadata.run_id,
        ingested_at=metadata.ingested_at,
        raw_json_path=str(result.raw_json_path),
        parquet_path=str(result.parquet_path),
        record_count=result.record_count,
        source_table=source_table,
    )


def _metadata_row_parameters(
    prepared: _PreparedFredDataset,
) -> tuple[object, ...]:
    metadata = prepared.result.location.metadata
    row = prepared.table.to_pylist()[0]
    return (
        metadata.source,
        metadata.dataset,
        metadata.run_id,
        0,
        row["series_id"],
        row["realtime_start"],
        row["realtime_end"],
        row["title"],
        row["observation_start"],
        row["observation_end"],
        row["frequency"],
        row["frequency_short"],
        row["units"],
        row["units_short"],
        row["seasonal_adjustment"],
        row["seasonal_adjustment_short"],
        row["last_updated"],
        row["popularity"],
        row["notes"],
    )


def _observation_row_parameters(
    prepared: _PreparedFredDataset,
) -> Iterable[tuple[object, ...]]:
    metadata = prepared.result.location.metadata
    for source_row_number, row in enumerate(prepared.table.to_pylist()):
        yield (
            metadata.source,
            metadata.dataset,
            metadata.run_id,
            source_row_number,
            row["series_id"],
            row["realtime_start"],
            row["realtime_end"],
            row["observation_date"],
            row["value"],
        )


def _insert_prepared_metadata(
    cursor: psycopg.Cursor,
    prepared: _PreparedFredDataset,
) -> None:
    cursor.execute(_INSERT_FRED_METADATA, _metadata_row_parameters(prepared))


def _insert_prepared_observations(
    cursor: psycopg.Cursor,
    prepared: _PreparedFredDataset,
) -> None:
    if prepared.result.record_count:
        cursor.executemany(
            _INSERT_FRED_OBSERVATION,
            _observation_row_parameters(prepared),
        )


def load_fred_metadata_bronze_to_postgres(
    connection: psycopg.Connection,
    result: FredBronzeDatasetResult,
) -> int:
    """Load one existing FRED metadata Bronze dataset."""
    prepared = _preflight_dataset(
        result,
        expected_dataset=FRED_SERIES_METADATA_BRONZE_DATASET,
        expected_schema=FRED_SERIES_METADATA_SCHEMA,
        require_exactly_one_row=True,
    )
    with connection.cursor() as cursor:
        try:
            replay_state = register_or_verify_ingestion_run(
                cursor,
                run=_ingestion_run(
                    prepared,
                    source_table="source_data.fred_series_metadata",
                ),
                insert_sql=_INSERT_INGESTION_RUN,
            )
        except IngestionRunReplayError as exc:
            raise FredPostgresLoadError(str(exc)) from exc
        if replay_state is IngestionRunState.INSERTED:
            _insert_prepared_metadata(cursor, prepared)
    return 1


def load_fred_observations_bronze_to_postgres(
    connection: psycopg.Connection,
    result: FredBronzeDatasetResult,
) -> int:
    """Load one existing FRED observations Bronze dataset."""
    prepared = _preflight_dataset(
        result,
        expected_dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
        expected_schema=FRED_SERIES_OBSERVATIONS_SCHEMA,
    )
    with connection.cursor() as cursor:
        try:
            replay_state = register_or_verify_ingestion_run(
                cursor,
                run=_ingestion_run(
                    prepared,
                    source_table="source_data.fred_series_observations",
                ),
                insert_sql=_INSERT_INGESTION_RUN,
            )
        except IngestionRunReplayError as exc:
            raise FredPostgresLoadError(str(exc)) from exc
        if replay_state is IngestionRunState.INSERTED:
            _insert_prepared_observations(cursor, prepared)
    return prepared.result.record_count


def load_fred_series_bronze_to_postgres(
    connection: psycopg.Connection,
    result: FredSeriesBronzeResult,
) -> FredPostgresLoadResult:
    """Load both FRED datasets under one caller-owned transaction boundary."""
    if type(result) is not FredSeriesBronzeResult:
        raise FredPostgresLoadError("result must be a FredSeriesBronzeResult")
    series_id = _validate_series_id(result.series_id)
    metadata = _preflight_dataset(
        result.metadata,
        expected_dataset=FRED_SERIES_METADATA_BRONZE_DATASET,
        expected_schema=FRED_SERIES_METADATA_SCHEMA,
        require_exactly_one_row=True,
    )
    observations = _preflight_dataset(
        result.observations,
        expected_dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
        expected_schema=FRED_SERIES_OBSERVATIONS_SCHEMA,
    )
    if (
        metadata.result.series_id != series_id
        or observations.result.series_id != series_id
    ):
        raise FredPostgresLoadError("FRED combined Bronze series IDs do not match")

    metadata_run = metadata.result.location.metadata
    observations_run = observations.result.location.metadata
    if (
        metadata_run.run_id != observations_run.run_id
        or metadata_run.ingested_at != observations_run.ingested_at
    ):
        raise FredPostgresLoadError("FRED combined Bronze run identity does not match")

    metadata_run = _ingestion_run(
        metadata,
        source_table="source_data.fred_series_metadata",
    )
    observations_run = _ingestion_run(
        observations,
        source_table="source_data.fred_series_observations",
    )

    with connection.cursor() as cursor:
        try:
            metadata_state = inspect_ingestion_run(cursor, run=metadata_run)
            observations_state = inspect_ingestion_run(cursor, run=observations_run)
            if metadata_state is not observations_state:
                raise FredPostgresLoadError(
                    "FRED combined PostgreSQL state is partially committed"
                )
            if metadata_state is IngestionRunState.VERIFIED_REPLAY:
                return FredPostgresLoadResult(
                    metadata_loaded=1,
                    observations_loaded=observations.result.record_count,
                )

            metadata_state = register_or_verify_ingestion_run(
                cursor,
                run=metadata_run,
                insert_sql=_INSERT_INGESTION_RUN,
            )
            observations_state = register_or_verify_ingestion_run(
                cursor,
                run=observations_run,
                insert_sql=_INSERT_INGESTION_RUN,
            )
        except IngestionRunReplayError as exc:
            raise FredPostgresLoadError(str(exc)) from exc
        if metadata_state is not observations_state:
            raise FredPostgresLoadError(
                "FRED combined PostgreSQL state changed to partially committed"
            )
        if metadata_state is IngestionRunState.INSERTED:
            _insert_prepared_metadata(cursor, metadata)
            _insert_prepared_observations(cursor, observations)

    return FredPostgresLoadResult(
        metadata_loaded=1,
        observations_loaded=observations.result.record_count,
    )
