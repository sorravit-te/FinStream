"""Conservative same-run recovery for immutable Bronze artifacts."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from finstream.bronze.json_storage import (
    BronzeRawJsonError,
    raw_json_path,
    read_raw_json,
)
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import (
    BronzeParquetError,
    parquet_path,
    read_parquet,
    write_parquet,
)


class BronzeRecoveryError(ValueError):
    """Raised when an existing Bronze run cannot be safely reused or rebuilt."""


@dataclass(frozen=True)
class BronzeRecoveryResult:
    """A verified or reconstructed canonical Bronze artifact pair."""

    raw_json_path: Path
    parquet_path: Path
    table: pa.Table
    reconstructed: bool


def recover_or_verify_bronze_artifacts(
    location: BronzeRunLocation,
    *,
    table_from_payload: Callable[[dict[str, object]], pa.Table],
) -> BronzeRecoveryResult | None:
    """Return ``None`` for no artifacts, or safely recover/verify one run.

    A valid raw-only run is reconstructed without contacting its provider. A
    complete pair must decode and exactly match the deterministic table rebuilt
    from raw JSON. All other existing states require explicit operator action.
    """
    raw_path = raw_json_path(location)
    structured_path = parquet_path(location)
    raw_exists = raw_path.is_file()
    parquet_exists = structured_path.is_file()

    if location.directory.exists():
        expected_names = {raw_path.name, structured_path.name}
        found_names = {path.name for path in location.directory.iterdir()}
        if not found_names.issubset(expected_names):
            raise BronzeRecoveryError(
                "Bronze run contains unexpected artifacts; operator action is required"
            )

    if not raw_exists and not parquet_exists:
        if raw_path.exists() or structured_path.exists():
            raise BronzeRecoveryError(
                "Bronze artifact path is not a regular file; operator action is required"
            )
        return None
    if not raw_exists:
        raise BronzeRecoveryError(
            "Bronze run has Parquet without Raw JSON; operator action is required"
        )
    if not parquet_exists and structured_path.exists():
        raise BronzeRecoveryError(
            "Bronze Parquet path is not a regular file; operator action is required"
        )

    try:
        expected_table = table_from_payload(read_raw_json(location))
        expected_table.validate(full=True)
    except (BronzeRawJsonError, ValueError, pa.ArrowException) as exc:
        raise BronzeRecoveryError(
            "Bronze Raw JSON cannot safely reconstruct this run"
        ) from exc

    if not parquet_exists:
        try:
            write_parquet(location, expected_table)
        except (BronzeParquetError, OSError) as exc:
            raise BronzeRecoveryError(
                "Bronze Parquet reconstruction could not complete"
            ) from exc
        return BronzeRecoveryResult(
            raw_json_path=raw_path,
            parquet_path=structured_path,
            table=expected_table,
            reconstructed=True,
        )

    try:
        stored_table = read_parquet(location)
        stored_table.validate(full=True)
    except (BronzeParquetError, OSError, pa.ArrowException) as exc:
        raise BronzeRecoveryError(
            "Bronze Parquet artifact is invalid; operator action is required"
        ) from exc
    if not stored_table.equals(expected_table, check_metadata=True):
        raise BronzeRecoveryError(
            "Bronze artifact pair does not match captured Raw JSON"
        )
    return BronzeRecoveryResult(
        raw_json_path=raw_path,
        parquet_path=structured_path,
        table=stored_table,
        reconstructed=False,
    )
