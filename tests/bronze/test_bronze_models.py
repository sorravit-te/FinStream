from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from finstream.bronze.models import BronzeIngestionMetadata, BronzeRunLocation
from finstream.bronze.paths import BronzePathValidationError


def test_metadata_is_frozen_and_preserves_valid_components() -> None:
    metadata = BronzeIngestionMetadata.from_run(
        source="fred",
        dataset="series_observations",
        ingested_at=datetime(2026, 8, 29, 12, 3, 5, tzinfo=timezone.utc),
    )

    assert metadata.source == "fred"
    assert metadata.dataset == "series_observations"
    with pytest.raises(FrozenInstanceError):
        metadata.source = "sec"  # type: ignore[misc]


def test_metadata_normalizes_timestamp_and_derives_run_id() -> None:
    timestamp = datetime(
        2026,
        8,
        29,
        19,
        3,
        5,
        123456,
        tzinfo=timezone(timedelta(hours=7)),
    )

    metadata = BronzeIngestionMetadata.from_run(
        source="sec",
        dataset="submissions",
        ingested_at=timestamp,
    )

    assert metadata.ingested_at == datetime(
        2026,
        8,
        29,
        12,
        3,
        5,
        123456,
        tzinfo=timezone.utc,
    )
    assert metadata.run_id == "20260829T120305123456Z"


def test_metadata_derives_an_entity_aware_run_id_without_changing_ingested_at() -> None:
    timestamp = datetime(2026, 8, 29, 19, 3, 5, 123456, tzinfo=timezone(timedelta(hours=7)))

    metadata = BronzeIngestionMetadata.from_run(
        source="twelve_data",
        dataset="daily_market_prices",
        ingested_at=timestamp,
        entity="AAPL",
    )

    assert metadata.ingested_at == datetime(2026, 8, 29, 12, 3, 5, 123456, tzinfo=timezone.utc)
    assert metadata.entity == "AAPL"
    assert metadata.run_id == "20260829T120305123456Z--entity-QUFQTA"


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("source", "FRED"),
        ("dataset", "../observations"),
    ],
)
def test_metadata_uses_shared_component_validation(
    field_name: str,
    value: str,
) -> None:
    arguments = {
        "source": "fred",
        "dataset": "series_observations",
    }
    arguments[field_name] = value

    with pytest.raises(BronzePathValidationError, match=field_name):
        BronzeIngestionMetadata.from_run(
            source=arguments["source"],
            dataset=arguments["dataset"],
            ingested_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
        )


def test_metadata_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone"):
        BronzeIngestionMetadata.from_run(
            source="fred",
            dataset="series_observations",
            ingested_at=datetime(2026, 8, 29, 12, 0),
        )


def test_location_factory_keeps_metadata_and_directory_aligned(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bronze"
    location = BronzeRunLocation.from_run(
        root=root,
        source="fred",
        dataset="series_observations",
        ingested_at=datetime(2026, 8, 29, 12, 3, 5, tzinfo=timezone.utc),
    )

    assert location.metadata.run_id == "20260829T120305000000Z"
    assert location.directory == (
        root
        / "fred"
        / "series_observations"
        / "ingestion_date=2026-08-29"
        / "run_id=20260829T120305000000Z"
    )
    assert not root.exists()


def test_location_rejects_directory_that_disagrees_with_metadata() -> None:
    metadata = BronzeIngestionMetadata.from_run(
        source="fred",
        dataset="series_observations",
        ingested_at=datetime(2026, 8, 29, 12, 3, 5, tzinfo=timezone.utc),
    )

    with pytest.raises(ValueError, match="does not match"):
        BronzeRunLocation(
            metadata=metadata,
            directory=Path("data/bronze/fred/wrong_dataset"),
        )


def test_location_is_frozen() -> None:
    location = BronzeRunLocation.from_run(
        source="fred",
        dataset="series_observations",
        ingested_at=datetime(2026, 8, 29, 12, 3, 5, tzinfo=timezone.utc),
    )

    with pytest.raises(FrozenInstanceError):
        location.directory = Path("other")  # type: ignore[misc]
