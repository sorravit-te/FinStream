"""Validation and parsing for SEC submissions payloads."""

import re
from datetime import date, datetime
from typing import Any

from finflow.sec.edgar import _normalize_cik
from finflow.sec.models import SecFilingMetadata, SecSubmissions


_ACCESSION_PATTERN = re.compile(r"\d{10}-\d{2}-\d{6}\Z")
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_RECENT_FIELDS = (
    "accessionNumber",
    "filingDate",
    "reportDate",
    "acceptanceDateTime",
    "act",
    "form",
    "fileNumber",
    "filmNumber",
    "items",
    "size",
    "isXBRL",
    "isInlineXBRL",
    "primaryDocument",
    "primaryDocDescription",
)


class SecSubmissionsValidationError(ValueError):
    """Raised when an SEC submissions payload contains invalid metadata."""


def _normalize_payload_cik(value: object, *, field_name: str) -> str:
    try:
        return _normalize_cik(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise SecSubmissionsValidationError(f"{field_name} is invalid") from exc


def _parse_date(value: object, *, field_name: str, optional: bool = False) -> date | None:
    if optional and value == "":
        return None
    if not isinstance(value, str) or not _DATE_PATTERN.fullmatch(value):
        raise SecSubmissionsValidationError(
            f"{field_name} must use YYYY-MM-DD format"
        )
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SecSubmissionsValidationError(f"{field_name} is not a valid date") from exc


def _parse_acceptance_datetime(value: object) -> datetime | None:
    if isinstance(value, str) and not value.strip():
        return None
    if not isinstance(value, str):
        raise SecSubmissionsValidationError("acceptanceDateTime must be a string")
    try:
        parsed_value = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SecSubmissionsValidationError(
            "acceptanceDateTime is not a valid ISO-8601 timestamp"
        ) from exc
    if parsed_value.tzinfo is None or parsed_value.utcoffset() is None:
        raise SecSubmissionsValidationError(
            "acceptanceDateTime must include a timezone"
        )
    return parsed_value


def _parse_accession_number(value: object) -> str:
    if not isinstance(value, str) or not _ACCESSION_PATTERN.fullmatch(value):
        raise SecSubmissionsValidationError(
            "accessionNumber must use XXXXXXXXXX-YY-ZZZZZZ format"
        )
    return value


def _parse_required_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SecSubmissionsValidationError(f"{field_name} must not be blank")
    return value


def _parse_optional_string(value: object, *, field_name: str) -> str | None:
    if not isinstance(value, str):
        raise SecSubmissionsValidationError(f"{field_name} must be a string")
    stripped_value = value.strip()
    return stripped_value or None


def _parse_size(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SecSubmissionsValidationError("size must be a non-negative integer")
    return value


def _parse_xbrl_flag(value: object, *, field_name: str) -> bool:
    if isinstance(value, bool) or not isinstance(value, int) or value not in (0, 1):
        raise SecSubmissionsValidationError(f"{field_name} must be 0 or 1")
    return bool(value)


def parse_submissions(
    payload: dict[str, Any],
    *,
    expected_cik: str | int,
) -> SecSubmissions:
    """Parse the aligned ``filings.recent`` columns in an SEC payload."""
    normalized_expected_cik = _normalize_payload_cik(
        expected_cik,
        field_name="expected_cik",
    )
    if not isinstance(payload, dict):
        raise SecSubmissionsValidationError("payload must be an object")

    normalized_payload_cik = _normalize_payload_cik(
        payload.get("cik"),
        field_name="payload.cik",
    )
    if normalized_payload_cik != normalized_expected_cik:
        raise SecSubmissionsValidationError("payload CIK does not match expected CIK")

    company_name = payload.get("name")
    if not isinstance(company_name, str) or not company_name.strip():
        raise SecSubmissionsValidationError("name must not be blank")

    filings_value = payload.get("filings")
    if not isinstance(filings_value, dict):
        raise SecSubmissionsValidationError("filings must be an object")
    recent_value = filings_value.get("recent")
    if not isinstance(recent_value, dict):
        raise SecSubmissionsValidationError("filings.recent must be an object")

    columns: dict[str, list[object]] = {}
    for field_name in _RECENT_FIELDS:
        field_value = recent_value.get(field_name)
        if not isinstance(field_value, list):
            raise SecSubmissionsValidationError(
                f"filings.recent.{field_name} must be a list"
            )
        columns[field_name] = field_value

    column_lengths = {len(column) for column in columns.values()}
    if len(column_lengths) != 1:
        raise SecSubmissionsValidationError(
            "filings.recent arrays must have equal lengths"
        )

    filings: list[SecFilingMetadata] = []
    seen_accession_numbers: set[str] = set()
    row_count = next(iter(column_lengths))
    for index in range(row_count):
        accession_number = _parse_accession_number(
            columns["accessionNumber"][index]
        )
        if accession_number in seen_accession_numbers:
            raise SecSubmissionsValidationError(
                "duplicate accessionNumber in filings.recent"
            )

        filing_date = _parse_date(
            columns["filingDate"][index],
            field_name="filingDate",
        )
        assert filing_date is not None
        filings.append(
            SecFilingMetadata(
                cik=normalized_payload_cik,
                accession_number=accession_number,
                filing_date=filing_date,
                report_date=_parse_date(
                    columns["reportDate"][index],
                    field_name="reportDate",
                    optional=True,
                ),
                acceptance_datetime=_parse_acceptance_datetime(
                    columns["acceptanceDateTime"][index]
                ),
                form=_parse_required_string(
                    columns["form"][index],
                    field_name="form",
                ),
                act=_parse_optional_string(
                    columns["act"][index],
                    field_name="act",
                ),
                file_number=_parse_optional_string(
                    columns["fileNumber"][index],
                    field_name="fileNumber",
                ),
                film_number=_parse_optional_string(
                    columns["filmNumber"][index],
                    field_name="filmNumber",
                ),
                items=_parse_optional_string(
                    columns["items"][index],
                    field_name="items",
                ),
                size=_parse_size(columns["size"][index]),
                is_xbrl=_parse_xbrl_flag(
                    columns["isXBRL"][index],
                    field_name="isXBRL",
                ),
                is_inline_xbrl=_parse_xbrl_flag(
                    columns["isInlineXBRL"][index],
                    field_name="isInlineXBRL",
                ),
                primary_document=_parse_optional_string(
                    columns["primaryDocument"][index],
                    field_name="primaryDocument",
                ),
                primary_doc_description=_parse_optional_string(
                    columns["primaryDocDescription"][index],
                    field_name="primaryDocDescription",
                ),
            )
        )
        seen_accession_numbers.add(accession_number)

    return SecSubmissions(
        cik=normalized_payload_cik,
        company_name=company_name.strip(),
        filings=tuple(filings),
    )
