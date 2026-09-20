"""Pure path construction for local Bronze storage."""

import base64
import re
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_BRONZE_ROOT = Path("data") / "bronze"

_COMPONENT_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9_]*[a-z0-9])?\Z")


class BronzePathValidationError(ValueError):
    """Raised when a Bronze path input violates the storage contract."""


def validate_bronze_path_component(value: object, *, field_name: str) -> str:
    """Return a valid canonical source or dataset path component."""
    if not isinstance(value, str) or not value.strip():
        raise BronzePathValidationError(f"{field_name} must be a non-blank string")
    if not _COMPONENT_PATTERN.fullmatch(value):
        raise BronzePathValidationError(
            f"{field_name} must contain only lowercase letters, digits, and "
            "interior underscores"
        )
    return value


def normalize_bronze_run_timestamp(timestamp: datetime) -> datetime:
    """Return a timezone-aware ingestion timestamp normalized to UTC."""
    if not isinstance(timestamp, datetime):
        raise ValueError("run timestamp must be a datetime")
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("run timestamp must include a timezone")
    return timestamp.astimezone(timezone.utc)


def validate_bronze_run_entity(entity: object) -> str:
    """Validate a caller-normalized source entity for a Bronze run identity."""
    if (
        not isinstance(entity, str)
        or not entity
        or entity != entity.strip()
    ):
        raise BronzePathValidationError(
            "entity must be a non-blank normalized string"
        )
    return entity


def encode_bronze_run_entity(entity: str) -> str:
    """Return a reversible path-safe representation of a normalized entity."""
    validated_entity = validate_bronze_run_entity(entity)
    return base64.urlsafe_b64encode(validated_entity.encode("utf-8")).decode(
        "ascii"
    ).rstrip("=")


def format_bronze_run_id(
    timestamp: datetime,
    *,
    entity: str | None = None,
) -> str:
    """Format a UTC ingestion instant with an optional source-entity suffix."""
    canonical_timestamp = normalize_bronze_run_timestamp(timestamp)
    timestamp_run_id = canonical_timestamp.strftime("%Y%m%dT%H%M%S%fZ")
    if entity is None:
        return timestamp_run_id
    return f"{timestamp_run_id}--entity-{encode_bronze_run_entity(entity)}"


def build_bronze_run_directory(
    *,
    root: str | Path = DEFAULT_BRONZE_ROOT,
    source: str,
    dataset: str,
    run_at: datetime,
    entity: str | None = None,
) -> Path:
    """Build the deterministic directory for one Bronze ingestion run."""
    canonical_timestamp = normalize_bronze_run_timestamp(run_at)
    source_component = validate_bronze_path_component(
        source,
        field_name="source",
    )
    dataset_component = validate_bronze_path_component(
        dataset,
        field_name="dataset",
    )
    return (
        Path(root)
        / source_component
        / dataset_component
        / f"ingestion_date={canonical_timestamp.date().isoformat()}"
        / f"run_id={format_bronze_run_id(canonical_timestamp, entity=entity)}"
    )
