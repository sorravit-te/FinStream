"""Local Raw JSON persistence for Bronze run locations."""

import json
import math
from pathlib import Path
from typing import Any

from finstream.bronze.models import BronzeRunLocation


RAW_JSON_FILENAME = "payload.json"


class BronzeRawJsonError(ValueError):
    """Base error for invalid Bronze Raw JSON artifacts."""


class BronzeRawJsonValidationError(BronzeRawJsonError):
    """Raised when a payload cannot be stored as strict JSON."""


class BronzeRawJsonReadError(BronzeRawJsonError):
    """Raised when a stored Raw JSON artifact is invalid."""


def raw_json_path(location: BronzeRunLocation) -> Path:
    """Return the fixed Raw JSON artifact path without filesystem access."""
    return location.directory / RAW_JSON_FILENAME


def _validate_strict_json(
    value: object,
    *,
    ancestors: set[int] | None = None,
) -> None:
    if isinstance(value, dict):
        if ancestors is None:
            ancestors = set()
        value_id = id(value)
        if value_id in ancestors:
            raise BronzeRawJsonValidationError(
                "Raw JSON payload cannot contain circular references"
            )
        ancestors.add(value_id)
        try:
            for key, item in value.items():
                if not isinstance(key, str):
                    raise BronzeRawJsonValidationError(
                        "Raw JSON object keys must be strings"
                    )
                _validate_strict_json(item, ancestors=ancestors)
        finally:
            ancestors.remove(value_id)
        return

    if isinstance(value, list):
        if ancestors is None:
            ancestors = set()
        value_id = id(value)
        if value_id in ancestors:
            raise BronzeRawJsonValidationError(
                "Raw JSON payload cannot contain circular references"
            )
        ancestors.add(value_id)
        try:
            for item in value:
                _validate_strict_json(item, ancestors=ancestors)
        finally:
            ancestors.remove(value_id)
        return

    if value is None or isinstance(value, (str, bool, int)):
        return

    if isinstance(value, float):
        if math.isfinite(value):
            return
        raise BronzeRawJsonValidationError(
            "Raw JSON payload cannot contain non-finite floats"
        )

    raise BronzeRawJsonValidationError(
        "Raw JSON payload contains an unsupported value type"
    )


def _serialize_raw_json(payload: object) -> str:
    if not isinstance(payload, dict):
        raise BronzeRawJsonValidationError("Raw JSON payload must be an object")
    _validate_strict_json(payload)
    try:
        serialized = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
        )
    except (TypeError, ValueError) as exc:
        raise BronzeRawJsonValidationError(
            "Raw JSON payload is not serializable"
        ) from exc
    return f"{serialized}\n"


def write_raw_json(
    location: BronzeRunLocation,
    payload: dict[str, Any],
) -> Path:
    """Persist one provider JSON object without overwriting an artifact."""
    serialized = _serialize_raw_json(payload)
    artifact_path = raw_json_path(location)
    location.directory.mkdir(parents=True, exist_ok=True)
    with artifact_path.open("x", encoding="utf-8", newline="\n") as artifact:
        artifact.write(serialized)
    return artifact_path


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"Non-finite JSON constant is not allowed: {value}")


def read_raw_json(location: BronzeRunLocation) -> dict[str, Any]:
    """Read and validate one stored provider JSON object."""
    artifact_path = raw_json_path(location)
    try:
        with artifact_path.open("r", encoding="utf-8") as artifact:
            payload = json.load(
                artifact,
                parse_constant=_reject_non_finite_constant,
            )
    except (UnicodeDecodeError, ValueError) as exc:
        raise BronzeRawJsonReadError("Stored Raw JSON artifact is invalid") from exc
    if not isinstance(payload, dict):
        raise BronzeRawJsonReadError("Stored Raw JSON payload must be an object")
    try:
        _validate_strict_json(payload)
    except BronzeRawJsonValidationError as exc:
        raise BronzeRawJsonReadError("Stored Raw JSON artifact is invalid") from exc
    return payload
