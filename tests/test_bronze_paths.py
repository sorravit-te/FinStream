from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from finstream.bronze.paths import (
    DEFAULT_BRONZE_ROOT,
    BronzePathValidationError,
    build_bronze_run_directory,
    format_bronze_run_id,
    validate_bronze_path_component,
)


_RUN_AT = datetime(2026, 8, 29, 12, 3, 5, 123456, tzinfo=timezone.utc)


def test_builds_default_bronze_run_hierarchy() -> None:
    result = build_bronze_run_directory(
        source="fred",
        dataset="series_observations",
        run_at=_RUN_AT,
    )

    assert isinstance(result, Path)
    assert result == (
        DEFAULT_BRONZE_ROOT
        / "fred"
        / "series_observations"
        / "ingestion_date=2026-08-29"
        / "run_id=20260829T120305123456Z"
    )


def test_builds_hierarchy_under_custom_string_root(tmp_path: Path) -> None:
    root = tmp_path / "configured" / "bronze"

    result = build_bronze_run_directory(
        root=str(root),
        source="sec",
        dataset="company_facts",
        run_at=_RUN_AT,
    )

    assert result.parts[-4:] == (
        "sec",
        "company_facts",
        "ingestion_date=2026-08-29",
        "run_id=20260829T120305123456Z",
    )
    assert result.parent.parent.parent.parent == root


def test_run_id_always_includes_microseconds() -> None:
    timestamp = datetime(2026, 8, 29, 12, 3, 5, tzinfo=timezone.utc)

    assert format_bronze_run_id(timestamp) == "20260829T120305000000Z"


def test_entity_aware_run_ids_are_distinct_deterministic_and_path_safe() -> None:
    aapl_run_id = format_bronze_run_id(_RUN_AT, entity="AAPL")
    msft_run_id = format_bronze_run_id(_RUN_AT, entity="MSFT")

    assert aapl_run_id == "20260829T120305123456Z--entity-QUFQTA"
    assert msft_run_id == "20260829T120305123456Z--entity-TVNGVA"
    assert aapl_run_id != msft_run_id
    assert format_bronze_run_id(_RUN_AT, entity="AAPL") == aapl_run_id
    assert "/" not in aapl_run_id
    assert "\\" not in aapl_run_id


def test_entity_aware_directory_keeps_the_shared_ingestion_timestamp() -> None:
    aapl_path = build_bronze_run_directory(
        source="twelve_data",
        dataset="daily_market_prices",
        run_at=_RUN_AT,
        entity="AAPL",
    )
    msft_path = build_bronze_run_directory(
        source="twelve_data",
        dataset="daily_market_prices",
        run_at=_RUN_AT,
        entity="MSFT",
    )

    assert aapl_path.parent == msft_path.parent
    assert aapl_path != msft_path
    assert aapl_path.name == "run_id=20260829T120305123456Z--entity-QUFQTA"


def test_non_utc_timestamp_normalizes_before_partitioning() -> None:
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

    result = build_bronze_run_directory(
        source="twelve_data",
        dataset="daily_market_prices",
        run_at=timestamp,
    )

    assert result.parts[-2:] == (
        "ingestion_date=2026-08-29",
        "run_id=20260829T120305123456Z",
    )


def test_timezone_conversion_uses_utc_ingestion_date() -> None:
    timestamp = datetime(
        2026,
        8,
        29,
        23,
        30,
        tzinfo=timezone(timedelta(hours=-2)),
    )

    result = build_bronze_run_directory(
        source="fred",
        dataset="series_metadata",
        run_at=timestamp,
    )

    assert result.parts[-2:] == (
        "ingestion_date=2026-08-30",
        "run_id=20260830T013000000000Z",
    )


def test_equivalent_instants_produce_identical_partitions() -> None:
    utc_timestamp = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
    offset_timestamp = datetime(
        2026,
        8,
        29,
        19,
        0,
        tzinfo=timezone(timedelta(hours=7)),
    )

    utc_path = build_bronze_run_directory(
        source="fred",
        dataset="series_observations",
        run_at=utc_timestamp,
    )
    offset_path = build_bronze_run_directory(
        source="fred",
        dataset="series_observations",
        run_at=offset_timestamp,
    )

    assert utc_path.parts[-2:] == offset_path.parts[-2:]
    assert format_bronze_run_id(utc_timestamp) == format_bronze_run_id(
        offset_timestamp
    )


def test_rejects_naive_run_timestamp() -> None:
    timestamp = datetime(2026, 8, 29, 12, 0)

    with pytest.raises(ValueError, match="timezone"):
        format_bronze_run_id(timestamp)
    with pytest.raises(ValueError, match="timezone"):
        build_bronze_run_directory(
            source="fred",
            dataset="series_observations",
            run_at=timestamp,
        )


def test_path_builder_has_no_filesystem_side_effects(tmp_path: Path) -> None:
    root = tmp_path / "missing" / "bronze"

    result = build_bronze_run_directory(
        root=root,
        source="fred",
        dataset="series_observations",
        run_at=_RUN_AT,
    )

    assert not root.exists()
    assert not result.exists()


@pytest.mark.parametrize(
    "component",
    [
        "fred",
        "sec",
        "twelve_data",
        "company_facts",
        "series_observations",
        "daily_market_prices",
        "x1",
    ],
)
def test_accepts_valid_path_components(component: str) -> None:
    assert validate_bronze_path_component(
        component,
        field_name="component",
    ) == component


@pytest.mark.parametrize("field_name", ["source", "dataset"])
@pytest.mark.parametrize(
    "component",
    [
        "",
        " ",
        "FRED",
        "fred data",
        "../fred",
        "fred/observations",
        "fred\\observations",
        "_fred",
        "fred_",
        "fred-data",
        ".",
        "..",
        None,
    ],
)
def test_rejects_invalid_source_and_dataset_components(
    field_name: str,
    component: object,
) -> None:
    arguments: dict[str, object] = {
        "source": "fred",
        "dataset": "series_observations",
    }
    arguments[field_name] = component

    with pytest.raises(BronzePathValidationError, match=field_name):
        build_bronze_run_directory(
            source=arguments["source"],  # type: ignore[arg-type]
            dataset=arguments["dataset"],  # type: ignore[arg-type]
            run_at=_RUN_AT,
        )
