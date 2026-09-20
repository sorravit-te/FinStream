from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

import finstream.bronze.parquet_storage as parquet_storage
from finstream.bronze.json_storage import write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import (
    STRUCTURED_PARQUET_FILENAME,
    BronzeParquetReadError,
    BronzeParquetValidationError,
    BronzeParquetWriteError,
    parquet_path,
    read_parquet,
    write_parquet,
)


_RUN_AT = datetime(2026, 8, 30, 12, 0, 0, tzinfo=timezone.utc)


def _location(tmp_path: Path) -> BronzeRunLocation:
    return BronzeRunLocation.from_run(
        root=tmp_path / "bronze",
        source="fred",
        dataset="series_observations",
        ingested_at=_RUN_AT,
    )


def _representative_table() -> pa.Table:
    schema = pa.schema(
        [
            ("name", pa.string()),
            ("count", pa.int64()),
            ("active", pa.bool_()),
            ("day", pa.date32()),
            ("observed_at", pa.timestamp("us", tz="UTC")),
            ("amount", pa.decimal128(18, 4)),
        ]
    )
    return pa.Table.from_pydict(
        {
            "name": ["alpha", None],
            "count": [1, None],
            "active": [True, None],
            "day": [date(2026, 8, 30), None],
            "observed_at": [
                datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc),
                None,
            ],
            "amount": [Decimal("12.3400"), None],
        },
        schema=schema,
    )


def test_parquet_path_uses_fixed_filename_without_side_effects(
    tmp_path: Path,
) -> None:
    location = _location(tmp_path)

    result = parquet_path(location)

    assert STRUCTURED_PARQUET_FILENAME == "data.parquet"
    assert result == location.directory / "data.parquet"
    assert not location.directory.exists()
    assert not result.exists()


def test_writes_and_reads_representative_typed_table(tmp_path: Path) -> None:
    location = _location(tmp_path)
    table = _representative_table()

    result = write_parquet(location, table)
    actual = read_parquet(location)

    assert result == parquet_path(location)
    assert result.is_file()
    assert isinstance(actual, pa.Table)
    assert actual.equals(table)
    assert actual.schema.equals(table.schema)
    assert actual.schema.names == table.schema.names
    assert actual.schema.field("amount").type == pa.decimal128(18, 4)
    assert actual.schema.field("observed_at").type == pa.timestamp("us", tz="UTC")


def test_writes_and_reads_zero_row_table_with_schema(tmp_path: Path) -> None:
    location = _location(tmp_path)
    schema = pa.schema(
        [
            ("series_id", pa.string()),
            ("observation_date", pa.date32()),
            ("value", pa.decimal128(18, 6)),
        ]
    )
    table = pa.Table.from_pylist([], schema=schema)

    write_parquet(location, table)

    actual = read_parquet(location)
    assert actual.num_rows == 0
    assert actual.schema.equals(schema)


@pytest.mark.parametrize(
    "table",
    [
        pytest.param(object(), id="non-arrow-table"),
        pytest.param(pa.table({}), id="no-fields"),
    ],
)
def test_invalid_table_fails_before_directory_creation(
    tmp_path: Path,
    table: object,
) -> None:
    location = _location(tmp_path)

    with pytest.raises(BronzeParquetValidationError):
        write_parquet(location, table)  # type: ignore[arg-type]

    assert not location.directory.exists()


def test_existing_artifact_is_not_overwritten(tmp_path: Path) -> None:
    location = _location(tmp_path)
    first_table = pa.table({"value": [1]})
    write_parquet(location, first_table)

    with pytest.raises(FileExistsError):
        write_parquet(location, pa.table({"value": [2]}))

    assert read_parquet(location).equals(first_table)


def test_corrupt_artifact_raises_bronze_read_error(tmp_path: Path) -> None:
    location = _location(tmp_path)
    location.directory.mkdir(parents=True)
    parquet_path(location).write_bytes(b"not a parquet artifact")

    with pytest.raises(BronzeParquetReadError) as error:
        read_parquet(location)

    assert error.value.__cause__ is not None


def test_missing_artifact_remains_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_parquet(_location(tmp_path))


def test_reader_does_not_infer_hive_parent_columns(tmp_path: Path) -> None:
    location = _location(tmp_path)
    table = pa.table({"value": [1]})

    write_parquet(location, table)

    assert read_parquet(location).column_names == ["value"]


def test_raw_json_and_parquet_coexist_in_one_run_directory(tmp_path: Path) -> None:
    location = _location(tmp_path)

    write_raw_json(location, {"source": "fred"})
    write_parquet(location, pa.table({"value": [1]}))

    assert sorted(path.name for path in location.directory.iterdir()) == [
        "data.parquet",
        "payload.json",
    ]


def test_failed_new_parquet_write_removes_partial_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    location = _location(tmp_path)

    def fail_write(*args: object, **kwargs: object) -> None:
        raise pa.ArrowInvalid("forced write failure")

    monkeypatch.setattr(parquet_storage.pq, "write_table", fail_write)

    with pytest.raises(BronzeParquetWriteError):
        write_parquet(location, pa.table({"value": [1]}))

    assert location.directory.is_dir()
    assert not parquet_path(location).exists()

