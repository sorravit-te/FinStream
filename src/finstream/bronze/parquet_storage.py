"""Local structured Parquet persistence for Bronze run locations."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from finstream.bronze.models import BronzeRunLocation


STRUCTURED_PARQUET_FILENAME = "data.parquet"


class BronzeParquetError(ValueError):
    """Base error for Bronze Parquet artifacts."""


class BronzeParquetValidationError(BronzeParquetError):
    """Raised when a supplied structured table is invalid."""


class BronzeParquetWriteError(BronzeParquetError):
    """Raised when Parquet encoding cannot complete."""


class BronzeParquetReadError(BronzeParquetError):
    """Raised when a stored Parquet artifact cannot be decoded."""


def parquet_path(location: BronzeRunLocation) -> Path:
    """Return the fixed Parquet artifact path without filesystem access."""
    return location.directory / STRUCTURED_PARQUET_FILENAME


def _validate_table(table: object) -> pa.Table:
    if not isinstance(table, pa.Table):
        raise BronzeParquetValidationError(
            "Structured Parquet input must be a pyarrow.Table"
        )
    if len(table.schema) == 0:
        raise BronzeParquetValidationError(
            "Structured Parquet table must contain at least one field"
        )
    try:
        table.validate(full=True)
    except pa.ArrowException as exc:
        raise BronzeParquetValidationError(
            "Structured Parquet table failed Arrow validation"
        ) from exc
    return table


def _remove_incomplete_artifact(artifact_path: Path) -> None:
    try:
        artifact_path.unlink()
    except FileNotFoundError:
        pass


def write_parquet(location: BronzeRunLocation, table: pa.Table) -> Path:
    """Persist one Arrow table as a collision-safe Bronze Parquet artifact."""
    table = _validate_table(table)
    artifact_path = parquet_path(location)
    location.directory.mkdir(parents=True, exist_ok=True)

    artifact_created = False
    try:
        with artifact_path.open("xb") as artifact:
            artifact_created = True
            try:
                pq.write_table(table, artifact, compression="snappy")
            except (pa.ArrowException, TypeError, ValueError) as exc:
                raise BronzeParquetWriteError(
                    "Structured Parquet artifact could not be written"
                ) from exc
    except (BronzeParquetWriteError, OSError):
        if artifact_created:
            _remove_incomplete_artifact(artifact_path)
        raise

    return artifact_path


def read_parquet(location: BronzeRunLocation) -> pa.Table:
    """Read one Parquet artifact without discovering parent partitions."""
    artifact_path = parquet_path(location)
    try:
        return pq.ParquetFile(artifact_path).read()
    except FileNotFoundError:
        raise
    except OSError:
        raise
    except pa.ArrowException as exc:
        raise BronzeParquetReadError(
            "Stored Structured Parquet artifact is invalid"
        ) from exc
