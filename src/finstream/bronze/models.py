"""Immutable metadata and locations for Bronze ingestion runs."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from finstream.bronze.paths import (
    DEFAULT_BRONZE_ROOT,
    build_bronze_run_directory,
    format_bronze_run_id,
    normalize_bronze_run_timestamp,
    validate_bronze_path_component,
)


@dataclass(frozen=True)
class BronzeIngestionMetadata:
    """Canonical source-independent metadata for one ingestion run."""

    source: str
    dataset: str
    ingested_at: datetime
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        source = validate_bronze_path_component(
            self.source,
            field_name="source",
        )
        dataset = validate_bronze_path_component(
            self.dataset,
            field_name="dataset",
        )
        ingested_at = normalize_bronze_run_timestamp(self.ingested_at)

        object.__setattr__(self, "source", source)
        object.__setattr__(self, "dataset", dataset)
        object.__setattr__(self, "ingested_at", ingested_at)
        object.__setattr__(self, "run_id", format_bronze_run_id(ingested_at))

    @classmethod
    def from_run(
        cls,
        *,
        source: str,
        dataset: str,
        ingested_at: datetime,
    ) -> "BronzeIngestionMetadata":
        """Create consistent canonical metadata for one ingestion instant."""
        return cls(
            source=source,
            dataset=dataset,
            ingested_at=ingested_at,
        )


@dataclass(frozen=True)
class BronzeRunLocation:
    """Bind canonical ingestion metadata to its deterministic directory."""

    metadata: BronzeIngestionMetadata
    directory: Path

    def __post_init__(self) -> None:
        directory = Path(self.directory)
        expected_suffix = build_bronze_run_directory(
            root=Path(),
            source=self.metadata.source,
            dataset=self.metadata.dataset,
            run_at=self.metadata.ingested_at,
        )
        if directory.parts[-len(expected_suffix.parts) :] != expected_suffix.parts:
            raise ValueError("Bronze directory does not match ingestion metadata")
        object.__setattr__(self, "directory", directory)

    @classmethod
    def from_run(
        cls,
        *,
        root: str | Path = DEFAULT_BRONZE_ROOT,
        source: str,
        dataset: str,
        ingested_at: datetime,
    ) -> "BronzeRunLocation":
        """Create canonical metadata and its matching Bronze directory."""
        metadata = BronzeIngestionMetadata.from_run(
            source=source,
            dataset=dataset,
            ingested_at=ingested_at,
        )
        return cls(
            metadata=metadata,
            directory=build_bronze_run_directory(
                root=root,
                source=metadata.source,
                dataset=metadata.dataset,
                run_at=metadata.ingested_at,
            ),
        )
