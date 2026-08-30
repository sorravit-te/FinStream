"""FRED-specific Bronze schemas, conversions, and result contracts."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

from finstream.bronze.models import BronzeRunLocation
from finstream.fred.models import (
    FredObservation,
    FredSeriesMetadata,
    FredSeriesObservations,
)


FRED_BRONZE_SOURCE = "fred"
FRED_SERIES_METADATA_BRONZE_DATASET = "series_metadata"
FRED_SERIES_OBSERVATIONS_BRONZE_DATASET = "series_observations"

FRED_SERIES_METADATA_SCHEMA = pa.schema(
    [
        pa.field("series_id", pa.string(), nullable=False),
        pa.field("realtime_start", pa.date32(), nullable=False),
        pa.field("realtime_end", pa.date32(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("observation_start", pa.date32(), nullable=False),
        pa.field("observation_end", pa.date32(), nullable=False),
        pa.field("frequency", pa.string(), nullable=False),
        pa.field("frequency_short", pa.string(), nullable=False),
        pa.field("units", pa.string(), nullable=False),
        pa.field("units_short", pa.string(), nullable=False),
        pa.field("seasonal_adjustment", pa.string(), nullable=False),
        pa.field("seasonal_adjustment_short", pa.string(), nullable=False),
        pa.field("last_updated", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("popularity", pa.int64(), nullable=False),
        pa.field("notes", pa.string(), nullable=True),
    ]
)

FRED_SERIES_OBSERVATIONS_SCHEMA = pa.schema(
    [
        pa.field("series_id", pa.string(), nullable=False),
        pa.field("realtime_start", pa.date32(), nullable=False),
        pa.field("realtime_end", pa.date32(), nullable=False),
        pa.field("observation_date", pa.date32(), nullable=False),
        pa.field("value", pa.decimal256(76, 30), nullable=True),
    ]
)


class FredBronzeValidationError(ValueError):
    """Raised when parsed FRED data cannot form a Bronze Arrow table."""


@dataclass(frozen=True)
class FredBronzeDatasetResult:
    """Locations and count for one persisted FRED endpoint dataset."""

    series_id: str
    location: BronzeRunLocation
    raw_json_path: Path
    parquet_path: Path
    record_count: int


@dataclass(frozen=True)
class FredSeriesBronzeResult:
    """Results for both FRED endpoint datasets for one series."""

    series_id: str
    metadata: FredBronzeDatasetResult
    observations: FredBronzeDatasetResult


def fred_series_metadata_to_table(metadata: FredSeriesMetadata) -> pa.Table:
    """Convert one parsed FRED metadata record to its stable Bronze schema."""
    if not isinstance(metadata, FredSeriesMetadata):
        raise FredBronzeValidationError("Expected a FredSeriesMetadata value")

    last_updated = metadata.last_updated
    if (
        not isinstance(last_updated, datetime)
        or last_updated.tzinfo is None
        or last_updated.utcoffset() is None
    ):
        raise FredBronzeValidationError(
            "FRED metadata last_updated must be a timezone-aware datetime"
        )

    row = {
        "series_id": metadata.series_id,
        "realtime_start": metadata.realtime_start,
        "realtime_end": metadata.realtime_end,
        "title": metadata.title,
        "observation_start": metadata.observation_start,
        "observation_end": metadata.observation_end,
        "frequency": metadata.frequency,
        "frequency_short": metadata.frequency_short,
        "units": metadata.units,
        "units_short": metadata.units_short,
        "seasonal_adjustment": metadata.seasonal_adjustment,
        "seasonal_adjustment_short": metadata.seasonal_adjustment_short,
        "last_updated": last_updated.astimezone(timezone.utc),
        "popularity": metadata.popularity,
        "notes": metadata.notes,
    }
    return _table_from_rows(
        [row],
        schema=FRED_SERIES_METADATA_SCHEMA,
        dataset_name="series metadata",
    )


def fred_series_observations_to_table(
    observations: FredSeriesObservations,
) -> pa.Table:
    """Convert parsed FRED observations to their stable occurrence-row schema."""
    if not isinstance(observations, FredSeriesObservations):
        raise FredBronzeValidationError("Expected a FredSeriesObservations value")

    rows: list[dict[str, object]] = []
    for index, observation in enumerate(observations.observations):
        if not isinstance(observation, FredObservation):
            raise FredBronzeValidationError(
                f"FRED observation {index} is not a FredObservation"
            )
        if observation.series_id != observations.series_id:
            raise FredBronzeValidationError(
                f"FRED observation {index} series_id does not match its collection"
            )
        rows.append(
            {
                "series_id": observation.series_id,
                "realtime_start": observation.realtime_start,
                "realtime_end": observation.realtime_end,
                "observation_date": observation.observation_date,
                "value": observation.value,
            }
        )

    return _table_from_rows(
        rows,
        schema=FRED_SERIES_OBSERVATIONS_SCHEMA,
        dataset_name="series observations",
    )


def _table_from_rows(
    rows: list[dict[str, object]],
    *,
    schema: pa.Schema,
    dataset_name: str,
) -> pa.Table:
    try:
        table = pa.Table.from_pylist(rows, schema=schema)
        table.validate(full=True)
    except (pa.ArrowException, TypeError, ValueError, OverflowError) as exc:
        raise FredBronzeValidationError(
            f"FRED {dataset_name} values cannot be represented by the Bronze schema"
        ) from exc
    return table
