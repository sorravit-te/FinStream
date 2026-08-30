import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from finstream.bronze.json_storage import (
    RAW_JSON_FILENAME,
    BronzeRawJsonReadError,
    BronzeRawJsonValidationError,
    raw_json_path,
    read_raw_json,
    write_raw_json,
)
from finstream.bronze.models import BronzeRunLocation


_RUN_AT = datetime(2026, 8, 29, 12, 3, 5, 123456, tzinfo=timezone.utc)


def _location(tmp_path: Path) -> BronzeRunLocation:
    return BronzeRunLocation.from_run(
        root=tmp_path / "bronze",
        source="fred",
        dataset="series_observations",
        ingested_at=_RUN_AT,
    )


def test_raw_json_path_uses_fixed_filename_without_side_effects(
    tmp_path: Path,
) -> None:
    location = _location(tmp_path)

    result = raw_json_path(location)

    assert RAW_JSON_FILENAME == "payload.json"
    assert result == location.directory / "payload.json"
    assert not location.directory.exists()
    assert not result.exists()


def test_writes_and_reads_complete_utf8_json_object(tmp_path: Path) -> None:
    location = _location(tmp_path)
    payload = {
        "message": "สวัสดี",
        "nested": {
            "items": [1, 2.5, "value", True, False, None],
            "object": {"preserved": "yes"},
        },
    }

    result = write_raw_json(location, payload)

    assert result == raw_json_path(location)
    assert location.directory.is_dir()
    assert result.is_file()
    assert read_raw_json(location) == payload
    text = result.read_text(encoding="utf-8")
    assert "สวัสดี" in text
    assert text.endswith("\n")
    assert not text.endswith("\n\n")


def test_preserves_nested_strict_json_types(tmp_path: Path) -> None:
    location = _location(tmp_path)
    payload = {
        "object": {"value": "source"},
        "array": [0, -1.5, True, False, None, {"nested": ["item"]}],
    }

    write_raw_json(location, payload)

    assert read_raw_json(location) == payload


def test_write_creates_only_expected_hierarchy_and_payload(tmp_path: Path) -> None:
    location = _location(tmp_path)
    root = tmp_path / "bronze"

    artifact_path = write_raw_json(location, {"value": "source"})

    files = [path for path in root.rglob("*") if path.is_file()]
    assert files == [artifact_path]
    assert artifact_path.name == "payload.json"
    assert not (location.directory / "metadata.json").exists()
    assert not (location.directory / "manifest.json").exists()


def test_read_rejects_malformed_json(tmp_path: Path) -> None:
    location = _location(tmp_path)
    location.directory.mkdir(parents=True)
    raw_json_path(location).write_text("{invalid", encoding="utf-8")

    with pytest.raises(BronzeRawJsonReadError, match="invalid") as error:
        read_raw_json(location)

    assert isinstance(error.value.__cause__, json.JSONDecodeError)


def test_read_rejects_valid_non_object_json(tmp_path: Path) -> None:
    location = _location(tmp_path)
    location.directory.mkdir(parents=True)
    raw_json_path(location).write_text('["value"]\n', encoding="utf-8")

    with pytest.raises(BronzeRawJsonReadError, match="must be an object"):
        read_raw_json(location)


def test_read_rejects_non_finite_json_constant(tmp_path: Path) -> None:
    location = _location(tmp_path)
    location.directory.mkdir(parents=True)
    raw_json_path(location).write_text('{"value": NaN}\n', encoding="utf-8")

    with pytest.raises(BronzeRawJsonReadError, match="invalid"):
        read_raw_json(location)


def test_read_rejects_non_finite_numeric_value(tmp_path: Path) -> None:
    location = _location(tmp_path)
    location.directory.mkdir(parents=True)
    raw_json_path(location).write_text('{"value": 1e400}\n', encoding="utf-8")

    with pytest.raises(BronzeRawJsonReadError, match="invalid"):
        read_raw_json(location)


def test_missing_artifact_remains_file_not_found(tmp_path: Path) -> None:
    location = _location(tmp_path)

    with pytest.raises(FileNotFoundError):
        read_raw_json(location)


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param([], id="non-object"),
        pytest.param({1: "value"}, id="integer-key"),
        pytest.param({"nested": {1: "value"}}, id="nested-integer-key"),
        pytest.param({"values": (1, 2)}, id="tuple"),
        pytest.param(
            {"nested": {"values": (1, 2)}},
            id="nested-tuple",
        ),
        pytest.param({"value": b"bytes"}, id="bytes"),
        pytest.param({"values": {1, 2}}, id="set"),
        pytest.param({"value": object()}, id="custom-object"),
        pytest.param({"value": float("nan")}, id="nan"),
        pytest.param({"value": float("inf")}, id="infinity"),
        pytest.param({"value": float("-inf")}, id="negative-infinity"),
    ],
)
def test_invalid_payload_fails_before_directory_creation(
    tmp_path: Path,
    payload: object,
) -> None:
    location = _location(tmp_path)

    with pytest.raises(BronzeRawJsonValidationError):
        write_raw_json(location, payload)  # type: ignore[arg-type]

    assert not location.directory.exists()
def test_existing_artifact_is_not_overwritten(tmp_path: Path) -> None:
    location = _location(tmp_path)
    artifact_path = write_raw_json(location, {"version": 1})
    original_contents = artifact_path.read_bytes()

    with pytest.raises(FileExistsError):
        write_raw_json(location, {"version": 2})

    assert artifact_path.read_bytes() == original_contents
    assert read_raw_json(location) == {"version": 1}
