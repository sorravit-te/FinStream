"""Validation and parsing for SEC submissions payloads."""

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from finstream.sec.edgar import _normalize_cik
from finstream.sec.models import (
    SecCompanyFacts,
    SecFilingMetadata,
    SecFinancialFact,
    SecSubmissions,
)


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


class SecCompanyFactsValidationError(ValueError):
    """Raised when an SEC Company Facts payload contains invalid facts."""


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


def _normalize_company_facts_cik(value: object, *, field_name: str) -> str:
    try:
        return _normalize_cik(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise SecCompanyFactsValidationError(f"{field_name} is invalid") from exc


def _parse_company_fact_date(value: object, *, field_name: str) -> date:
    if not isinstance(value, str) or not _DATE_PATTERN.fullmatch(value):
        raise SecCompanyFactsValidationError(
            f"{field_name} must use YYYY-MM-DD format"
        )
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SecCompanyFactsValidationError(
            f"{field_name} is not a valid date"
        ) from exc


def _parse_fact_value(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SecCompanyFactsValidationError("val must be a JSON number")
    parsed_value = Decimal(value) if isinstance(value, int) else Decimal(str(value))
    if not parsed_value.is_finite():
        raise SecCompanyFactsValidationError("val must be finite")
    return parsed_value


def _parse_company_accession_number(value: object) -> str:
    if not isinstance(value, str) or not _ACCESSION_PATTERN.fullmatch(value):
        raise SecCompanyFactsValidationError(
            "accn must use XXXXXXXXXX-YY-ZZZZZZ format"
        )
    return value


def _parse_fiscal_year(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SecCompanyFactsValidationError("fy must be a positive integer")
    return value


def _parse_optional_fact_string(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise SecCompanyFactsValidationError(f"{field_name} must be a string")
    stripped_value = value.strip()
    return stripped_value or None


def _parse_fact_occurrence(
    value: object,
    *,
    cik: str,
    taxonomy: str,
    concept: str,
    label: str | None,
    description: str | None,
    unit: str,
) -> SecFinancialFact:
    if not isinstance(value, dict):
        raise SecCompanyFactsValidationError("fact occurrence must be an object")

    start_date = None
    if "start" in value:
        start_date = _parse_company_fact_date(value["start"], field_name="start")
    end_date = _parse_company_fact_date(value.get("end"), field_name="end")
    if start_date is not None and start_date > end_date:
        raise SecCompanyFactsValidationError("start must not be after end")

    form = value.get("form")
    if not isinstance(form, str) or not form.strip():
        raise SecCompanyFactsValidationError("form must not be blank")

    return SecFinancialFact(
        cik=cik,
        taxonomy=taxonomy,
        concept=concept,
        label=label,
        description=description,
        unit=unit,
        value=_parse_fact_value(value.get("val")),
        start_date=start_date,
        end_date=end_date,
        accession_number=_parse_company_accession_number(value.get("accn")),
        fiscal_year=_parse_fiscal_year(value.get("fy")),
        fiscal_period=_parse_optional_fact_string(value.get("fp"), field_name="fp"),
        form=form.strip(),
        filed_date=_parse_company_fact_date(value.get("filed"), field_name="filed"),
        frame=_parse_optional_fact_string(value.get("frame"), field_name="frame"),
    )


def parse_company_facts(
    payload: dict[str, Any],
    *,
    expected_cik: str | int,
) -> SecCompanyFacts:
    """Flatten a validated SEC Company Facts payload in provider order."""
    normalized_expected_cik = _normalize_company_facts_cik(
        expected_cik,
        field_name="expected_cik",
    )
    if not isinstance(payload, dict):
        raise SecCompanyFactsValidationError("payload must be an object")

    normalized_payload_cik = _normalize_company_facts_cik(
        payload.get("cik"),
        field_name="payload.cik",
    )
    if normalized_payload_cik != normalized_expected_cik:
        raise SecCompanyFactsValidationError(
            "payload CIK does not match expected CIK"
        )

    entity_name = payload.get("entityName")
    if not isinstance(entity_name, str) or not entity_name.strip():
        raise SecCompanyFactsValidationError("entityName must not be blank")
    facts_value = payload.get("facts")
    if not isinstance(facts_value, dict):
        raise SecCompanyFactsValidationError("facts must be an object")

    parsed_facts: list[SecFinancialFact] = []
    for taxonomy, taxonomy_value in facts_value.items():
        if not isinstance(taxonomy, str) or not taxonomy.strip():
            raise SecCompanyFactsValidationError("taxonomy must not be blank")
        if not isinstance(taxonomy_value, dict):
            raise SecCompanyFactsValidationError("taxonomy value must be an object")

        for concept, concept_value in taxonomy_value.items():
            if not isinstance(concept, str) or not concept.strip():
                raise SecCompanyFactsValidationError("concept must not be blank")
            if not isinstance(concept_value, dict):
                raise SecCompanyFactsValidationError("concept value must be an object")

            if "label" not in concept_value:
                raise SecCompanyFactsValidationError("label must not be blank")
            label_value = concept_value["label"]
            if label_value is None:
                label = None
            elif isinstance(label_value, str):
                label = label_value.strip() or None
            else:
                raise SecCompanyFactsValidationError("label must not be blank")
            if "description" not in concept_value:
                raise SecCompanyFactsValidationError("description must be a string")
            description_value = concept_value["description"]
            if description_value is None:
                description = None
            elif isinstance(description_value, str):
                description = description_value if description_value.strip() else None
            else:
                raise SecCompanyFactsValidationError("description must be a string")

            units_value = concept_value.get("units")
            if not isinstance(units_value, dict):
                raise SecCompanyFactsValidationError("units must be an object")
            for unit, occurrences in units_value.items():
                if not isinstance(unit, str) or not unit.strip():
                    raise SecCompanyFactsValidationError("unit must not be blank")
                if not isinstance(occurrences, list):
                    raise SecCompanyFactsValidationError("unit value must be a list")
                for occurrence in occurrences:
                    parsed_facts.append(
                        _parse_fact_occurrence(
                            occurrence,
                            cik=normalized_payload_cik,
                            taxonomy=taxonomy,
                            concept=concept,
                            label=label,
                            description=description,
                            unit=unit,
                        )
                    )

    return SecCompanyFacts(
        cik=normalized_payload_cik,
        entity_name=entity_name.strip(),
        facts=tuple(parsed_facts),
    )
