from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, call

import pyarrow as pa
import pytest

import finstream.fred.ingestion as fred_ingestion
from finstream.bronze.json_storage import BronzeRawJsonValidationError, read_raw_json, write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.recovery import BronzeRecoveryError
from finstream.bronze.parquet_storage import BronzeParquetWriteError, read_parquet, write_parquet
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE, FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_METADATA_SCHEMA, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_SCHEMA, FredBronzeValidationError,
    fred_series_metadata_to_table, fred_series_observations_to_table,
)
from finstream.fred.client import FredClient, FredError
from finstream.fred.ingestion import FredMacroeconomicIngestionService
from finstream.fred.models import FredObservation, FredSeriesMetadata, FredSeriesObservations
from finstream.fred.parsing import (
    FredObservationValidationError, FredSeriesMetadataValidationError,
    parse_series_metadata, parse_series_observations,
)

RUN_AT = datetime(2026, 8, 30, 12, 0, 0, 123456, tzinfo=timezone.utc)
SERIES_ID = "DFF"


def metadata_payload(last_updated: str = "2026-08-29 12:00:00+00:00") -> dict:
    return {"seriess": [{
        "id": SERIES_ID, "realtime_start": "2026-08-01", "realtime_end": "2026-08-29",
        "title": "Federal Funds Effective Rate", "observation_start": "1954-07-01",
        "observation_end": "2026-08-28", "frequency": "Daily", "frequency_short": "D",
        "units": "Percent", "units_short": "%", "seasonal_adjustment": "Not Seasonally Adjusted",
        "seasonal_adjustment_short": "NSA", "last_updated": last_updated, "popularity": 99,
        "notes": None,
    }]}


def observation(value: str = "4.33", day: str = "2026-08-28") -> dict:
    return {"realtime_start": "2026-08-29", "realtime_end": "2026-08-29", "date": day, "value": value}


def observations_payload(rows: list[dict] | None = None) -> dict:
    return {"observations": [] if rows is None else rows}


def location(tmp_path: Path, dataset: str) -> BronzeRunLocation:
    return BronzeRunLocation.from_run(root=tmp_path / "bronze", source=FRED_BRONZE_SOURCE, dataset=dataset, ingested_at=RUN_AT)


def client(metadata: object | None = None, observations: object | None = None) -> Mock:
    result = Mock(spec=FredClient)
    result.fetch_series.return_value = metadata_payload() if metadata is None else metadata
    result.fetch_series_observations.return_value = observations_payload([observation()]) if observations is None else observations
    return result


def test_constants_locations_and_explicit_schemas(tmp_path: Path) -> None:
    metadata = location(tmp_path, FRED_SERIES_METADATA_BRONZE_DATASET)
    observations = location(tmp_path, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    assert (FRED_BRONZE_SOURCE, FRED_SERIES_METADATA_BRONZE_DATASET, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET) == ("fred", "series_metadata", "series_observations")
    assert metadata.metadata.run_id == observations.metadata.run_id
    assert metadata.directory != observations.directory
    assert FRED_SERIES_METADATA_SCHEMA.names == ["series_id", "realtime_start", "realtime_end", "title", "observation_start", "observation_end", "frequency", "frequency_short", "units", "units_short", "seasonal_adjustment", "seasonal_adjustment_short", "last_updated", "popularity", "notes"]
    assert FRED_SERIES_METADATA_SCHEMA.types == [pa.string(), pa.date32(), pa.date32(), pa.string(), pa.date32(), pa.date32(), pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.timestamp("us", tz="UTC"), pa.int64(), pa.string()]
    assert [field.nullable for field in FRED_SERIES_METADATA_SCHEMA] == [False] * 14 + [True]
    assert FRED_SERIES_OBSERVATIONS_SCHEMA.names == ["series_id", "realtime_start", "realtime_end", "observation_date", "value"]
    assert FRED_SERIES_OBSERVATIONS_SCHEMA.types == [pa.string(), pa.date32(), pa.date32(), pa.date32(), pa.decimal256(76, 30)]
    assert [field.nullable for field in FRED_SERIES_OBSERVATIONS_SCHEMA] == [False, False, False, False, True]


def test_metadata_conversion_preserves_values_and_normalizes_utc() -> None:
    parsed = parse_series_metadata(metadata_payload("2026-08-29 19:00:00+07:00"), expected_series_id=SERIES_ID)
    table = fred_series_metadata_to_table(parsed)
    assert table.schema.equals(FRED_SERIES_METADATA_SCHEMA)
    assert table.num_rows == 1
    assert table.column("last_updated").cast(pa.timestamp("us")).to_pylist() == [datetime(2026, 8, 29, 12, 0)]
    assert table.column("notes").to_pylist() == [None]
    with pytest.raises(FredBronzeValidationError):
        fred_series_metadata_to_table(object())  # type: ignore[arg-type]
    with pytest.raises(FredBronzeValidationError):
        fred_series_metadata_to_table(FredSeriesMetadata(**{**parsed.__dict__, "popularity": object()}))  # type: ignore[arg-type]


def test_observation_conversion_preserves_order_decimal_missing_and_empty() -> None:
    parsed = parse_series_observations(observations_payload([observation("12345.678900000000000000000000000001"), observation(".", "2026-08-27")]), expected_series_id=SERIES_ID)
    table = fred_series_observations_to_table(parsed)
    assert table.schema.equals(FRED_SERIES_OBSERVATIONS_SCHEMA)
    assert table.column("observation_date").to_pylist() == [date(2026, 8, 28), date(2026, 8, 27)]
    assert table.column("value").to_pylist() == [Decimal("12345.678900000000000000000000000001"), None]
    empty = fred_series_observations_to_table(FredSeriesObservations(SERIES_ID, ()))
    assert empty.num_rows == 0 and empty.schema.equals(FRED_SERIES_OBSERVATIONS_SCHEMA)


@pytest.mark.parametrize("wrapped", [
    FredSeriesObservations(SERIES_ID, (object(),)),  # type: ignore[arg-type]
    FredSeriesObservations("DFF", (FredObservation("UNRATE", date(2026, 8, 29), date(2026, 8, 29), date(2026, 8, 28), Decimal("4.33")),)),
    FredSeriesObservations("DFF", (FredObservation("DFF", date(2026, 8, 29), date(2026, 8, 29), date(2026, 8, 28), Decimal("0.0000000000000000000000000000001")),)),
])
def test_observation_converter_rejects_invalid_nested_values(wrapped: FredSeriesObservations) -> None:
    with pytest.raises(FredBronzeValidationError):
        fred_series_observations_to_table(wrapped)


def test_metadata_and_observation_endpoints_store_exact_reprocessable_raw(tmp_path: Path) -> None:
    metadata = metadata_payload("2026-08-29 19:00:00+07:00")
    observations = observations_payload([observation(".")])
    source = client(metadata, observations)
    service = FredMacroeconomicIngestionService(source)
    metadata_result = service.ingest_metadata_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "bronze")
    boundaries = {"observation_start": date(2026, 1, 1), "observation_end": date(2026, 8, 28), "realtime_start": date(2026, 8, 1), "realtime_end": date(2026, 8, 29)}
    observation_result = service.ingest_observations_to_bronze(SERIES_ID, run_at=RUN_AT.replace(microsecond=123457), bronze_root=tmp_path / "bronze", **boundaries)
    source.fetch_series.assert_called_once_with(SERIES_ID)
    source.fetch_series_observations.assert_called_once_with(SERIES_ID, **boundaries)
    assert read_raw_json(metadata_result.location) == metadata
    assert read_raw_json(observation_result.location) == observations
    assert read_raw_json(metadata_result.location)["seriess"][0]["last_updated"] == "2026-08-29 19:00:00+07:00"
    assert read_raw_json(observation_result.location)["observations"][0]["value"] == "."
    assert read_parquet(observation_result.location).column("value").to_pylist() == [None]
    assert parse_series_metadata(read_raw_json(metadata_result.location), expected_series_id=SERIES_ID) == parse_series_metadata(metadata, expected_series_id=SERIES_ID)
    assert parse_series_observations(read_raw_json(observation_result.location), expected_series_id=SERIES_ID) == parse_series_observations(observations, expected_series_id=SERIES_ID)
    source.fetch_series.assert_called_once()
    source.fetch_series_observations.assert_called_once()


def test_empty_observations_and_complete_series_order(tmp_path: Path) -> None:
    source = client(observations=observations_payload())
    service = FredMacroeconomicIngestionService(source)
    result = service.ingest_series_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "bronze")
    assert source.method_calls == [call.fetch_series(SERIES_ID), call.fetch_series_observations(SERIES_ID, observation_start=None, observation_end=None, realtime_start=None, realtime_end=None)]
    assert result.metadata.location.metadata.run_id == result.observations.location.metadata.run_id
    assert result.observations.record_count == 0
    assert read_parquet(result.observations.location).schema.equals(FRED_SERIES_OBSERVATIONS_SCHEMA)
    assert read_parquet(result.observations.location).num_rows == 0


def test_same_run_raw_only_fred_datasets_reconstruct_without_requests(tmp_path: Path) -> None:
    metadata_location = location(tmp_path, FRED_SERIES_METADATA_BRONZE_DATASET)
    observations_location = location(tmp_path, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    write_raw_json(metadata_location, metadata_payload())
    write_raw_json(observations_location, observations_payload([observation()]))
    source = client()

    result = FredMacroeconomicIngestionService(source).ingest_series_to_bronze(
        SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "bronze"
    )

    assert result.metadata.record_count == 1
    assert result.observations.record_count == 1
    assert result.metadata.parquet_path.is_file()
    assert result.observations.parquet_path.is_file()
    reused = FredMacroeconomicIngestionService(source).ingest_series_to_bronze(
        SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "bronze"
    )
    assert reused.metadata.record_count == 1
    assert reused.observations.record_count == 1
    assert source.method_calls == []


@pytest.mark.parametrize("dataset", [FRED_SERIES_METADATA_BRONZE_DATASET, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET])
def test_complete_series_preflight_collision_prevents_requests(tmp_path: Path, dataset: str) -> None:
    existing = location(tmp_path, dataset)
    write_raw_json(existing, {"existing": "artifact"})
    source = client()
    with pytest.raises(BronzeRecoveryError):
        FredMacroeconomicIngestionService(source).ingest_series_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "bronze")
    assert source.method_calls == []
    assert read_raw_json(existing) == {"existing": "artifact"}


@pytest.mark.parametrize("kwargs", [
    {"series_id": " "},
    {"observation_start": date(2026, 2, 1), "observation_end": date(2026, 1, 1)},
    {"realtime_start": date(2026, 2, 1), "realtime_end": date(2026, 1, 1)},
    {"run_at": datetime(2026, 8, 30, 12, 0)},
])
def test_local_prevalidation_prevents_requests(tmp_path: Path, kwargs: dict[str, object]) -> None:
    source = client()
    arguments = {"series_id": SERIES_ID, "run_at": RUN_AT, "bronze_root": tmp_path / "bronze", **kwargs}
    with pytest.raises(ValueError):
        FredMacroeconomicIngestionService(source).ingest_observations_to_bronze(**arguments)  # type: ignore[arg-type]
    assert source.method_calls == []


def test_metadata_failures_do_not_request_observations_or_create_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name, supplied, expected in [("provider", FredError("failed"), FredError), ("parser", {"seriess": []}, FredSeriesMetadataValidationError)]:
        source = client(metadata=supplied)
        if isinstance(supplied, Exception):
            source.fetch_series.side_effect = supplied
        with pytest.raises(expected):
            FredMacroeconomicIngestionService(source).ingest_series_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / name)
        source.fetch_series_observations.assert_not_called()
        assert not (tmp_path / name).exists()
    source = client()
    monkeypatch.setattr(fred_ingestion, "fred_series_metadata_to_table", Mock(side_effect=FredBronzeValidationError("arrow")))
    with pytest.raises(FredBronzeValidationError):
        FredMacroeconomicIngestionService(source).ingest_series_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "arrow")
    assert not (tmp_path / "arrow").exists()


def test_storage_failure_ordering_and_later_observation_partial_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = client()
    parquet = Mock()
    monkeypatch.setattr(fred_ingestion, "write_raw_json", Mock(side_effect=BronzeRawJsonValidationError("raw")))
    monkeypatch.setattr(fred_ingestion, "write_parquet", parquet)
    with pytest.raises(BronzeRawJsonValidationError):
        FredMacroeconomicIngestionService(source).ingest_metadata_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "raw")
    parquet.assert_not_called()
    monkeypatch.setattr(fred_ingestion, "write_raw_json", write_raw_json)
    monkeypatch.setattr(fred_ingestion, "write_parquet", write_parquet)

    source = client(observations={"observations": [{}]})
    with pytest.raises(FredObservationValidationError):
        FredMacroeconomicIngestionService(source).ingest_series_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "parser" / "bronze")
    assert read_parquet(location(tmp_path / "parser", FRED_SERIES_METADATA_BRONZE_DATASET)).num_rows == 1
    assert not location(tmp_path / "parser", FRED_SERIES_OBSERVATIONS_BRONZE_DATASET).directory.exists()

    source = client()
    real_parquet = fred_ingestion.write_parquet
    def fail_observation_parquet(target: BronzeRunLocation, table: pa.Table) -> Path:
        if target.metadata.dataset == FRED_SERIES_OBSERVATIONS_BRONZE_DATASET:
            raise BronzeParquetWriteError("observation parquet")
        return real_parquet(target, table)
    monkeypatch.setattr(fred_ingestion, "write_raw_json", write_raw_json)
    monkeypatch.setattr(fred_ingestion, "write_parquet", fail_observation_parquet)
    with pytest.raises(BronzeParquetWriteError):
        FredMacroeconomicIngestionService(source).ingest_series_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "parquet" / "bronze")
    assert read_raw_json(location(tmp_path / "parquet", FRED_SERIES_METADATA_BRONZE_DATASET)) == metadata_payload()
    assert read_raw_json(location(tmp_path / "parquet", FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)) == observations_payload([observation()])

@pytest.mark.parametrize("dataset", [FRED_SERIES_METADATA_BRONZE_DATASET, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET])
def test_endpoint_collision_prevents_its_provider_request(tmp_path: Path, dataset: str) -> None:
    existing = location(tmp_path, dataset)
    write_raw_json(existing, {"existing": "artifact"})
    source = client()
    service = FredMacroeconomicIngestionService(source)
    with pytest.raises(BronzeRecoveryError):
        if dataset == FRED_SERIES_METADATA_BRONZE_DATASET:
            service.ingest_metadata_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "bronze")
        else:
            service.ingest_observations_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "bronze")
    assert source.method_calls == []
    assert read_raw_json(existing) == {"existing": "artifact"}


def test_later_observation_conversion_and_raw_failures_retain_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = client()
    monkeypatch.setattr(fred_ingestion, "fred_series_observations_to_table", Mock(side_effect=FredBronzeValidationError("arrow")))
    with pytest.raises(FredBronzeValidationError):
        FredMacroeconomicIngestionService(source).ingest_series_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "arrow" / "bronze")
    assert read_parquet(location(tmp_path / "arrow", FRED_SERIES_METADATA_BRONZE_DATASET)).num_rows == 1
    assert not location(tmp_path / "arrow", FRED_SERIES_OBSERVATIONS_BRONZE_DATASET).directory.exists()

    source = client()
    real_raw_writer = fred_ingestion.write_raw_json
    parquet_writer = Mock(side_effect=fred_ingestion.write_parquet)
    monkeypatch.setattr(fred_ingestion, "fred_series_observations_to_table", fred_series_observations_to_table)
    def fail_observations_raw(target: BronzeRunLocation, payload: dict) -> Path:
        if target.metadata.dataset == FRED_SERIES_OBSERVATIONS_BRONZE_DATASET:
            raise BronzeRawJsonValidationError("observation raw")
        return real_raw_writer(target, payload)
    monkeypatch.setattr(fred_ingestion, "write_raw_json", fail_observations_raw)
    monkeypatch.setattr(fred_ingestion, "write_parquet", parquet_writer)
    with pytest.raises(BronzeRawJsonValidationError):
        FredMacroeconomicIngestionService(source).ingest_series_to_bronze(SERIES_ID, run_at=RUN_AT, bronze_root=tmp_path / "raw-observation" / "bronze")
    assert parquet_writer.call_count == 1
    assert read_parquet(location(tmp_path / "raw-observation", FRED_SERIES_METADATA_BRONZE_DATASET)).num_rows == 1

def test_metadata_parquet_failure_retains_raw_and_prevents_observation_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = metadata_payload()
    source = client(metadata=metadata)
    real_write_parquet = fred_ingestion.write_parquet

    def fail_metadata_parquet(
        target: BronzeRunLocation,
        table: pa.Table,
    ) -> Path:
        if target.metadata.dataset == FRED_SERIES_METADATA_BRONZE_DATASET:
            raise BronzeParquetWriteError("metadata parquet failed")
        return real_write_parquet(target, table)

    monkeypatch.setattr(fred_ingestion, "write_parquet", fail_metadata_parquet)

    with pytest.raises(BronzeParquetWriteError, match="metadata parquet failed"):
        FredMacroeconomicIngestionService(source).ingest_series_to_bronze(
            SERIES_ID,
            run_at=RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    metadata_location = location(tmp_path, FRED_SERIES_METADATA_BRONZE_DATASET)
    observations_location = location(tmp_path, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    source.fetch_series.assert_called_once_with(SERIES_ID)
    source.fetch_series_observations.assert_not_called()
    assert read_raw_json(metadata_location) == metadata
    assert not (metadata_location.directory / "data.parquet").exists()
    assert not observations_location.directory.exists()


def test_observation_provider_failure_retains_completed_metadata_only(
    tmp_path: Path,
) -> None:
    metadata = metadata_payload()
    source = client(metadata=metadata)
    source.fetch_series_observations.side_effect = FredError("observations failed")

    with pytest.raises(FredError, match="observations failed"):
        FredMacroeconomicIngestionService(source).ingest_series_to_bronze(
            SERIES_ID,
            run_at=RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    metadata_location = location(tmp_path, FRED_SERIES_METADATA_BRONZE_DATASET)
    observations_location = location(tmp_path, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    assert source.method_calls == [
        call.fetch_series(SERIES_ID),
        call.fetch_series_observations(
            SERIES_ID,
            observation_start=None,
            observation_end=None,
            realtime_start=None,
            realtime_end=None,
        ),
    ]
    assert read_raw_json(metadata_location) == metadata
    assert read_parquet(metadata_location).num_rows == 1
    assert not (observations_location.directory / "payload.json").exists()
    assert not (observations_location.directory / "data.parquet").exists()
