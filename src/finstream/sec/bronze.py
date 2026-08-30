"""SEC-specific Bronze schemas, conversions, and result contracts."""

from dataclasses import dataclass
from datetime import timezone
from pathlib import Path

import pyarrow as pa

from finstream.bronze.models import BronzeRunLocation
from finstream.sec.models import (
    SecCompanyFacts,
    SecFinancialFact,
    SecFilingMetadata,
    SecSubmissions,
)


SEC_BRONZE_SOURCE = "sec_edgar"
SEC_SUBMISSIONS_BRONZE_DATASET = "submissions"
SEC_COMPANY_FACTS_BRONZE_DATASET = "company_facts"

SEC_SUBMISSIONS_SCHEMA = pa.schema(
    [
        pa.field("cik", pa.string(), nullable=False),
        pa.field("company_name", pa.string(), nullable=False),
        pa.field("accession_number", pa.string(), nullable=False),
        pa.field("filing_date", pa.date32(), nullable=False),
        pa.field("report_date", pa.date32(), nullable=True),
        pa.field(
            "acceptance_datetime",
            pa.timestamp("us", tz="UTC"),
            nullable=True,
        ),
        pa.field("form", pa.string(), nullable=False),
        pa.field("act", pa.string(), nullable=True),
        pa.field("file_number", pa.string(), nullable=True),
        pa.field("film_number", pa.string(), nullable=True),
        pa.field("items", pa.string(), nullable=True),
        pa.field("size", pa.int64(), nullable=False),
        pa.field("is_xbrl", pa.bool_(), nullable=False),
        pa.field("is_inline_xbrl", pa.bool_(), nullable=False),
        pa.field("primary_document", pa.string(), nullable=True),
        pa.field("primary_doc_description", pa.string(), nullable=True),
    ]
)

SEC_COMPANY_FACTS_SCHEMA = pa.schema(
    [
        pa.field("cik", pa.string(), nullable=False),
        pa.field("entity_name", pa.string(), nullable=False),
        pa.field("taxonomy", pa.string(), nullable=False),
        pa.field("concept", pa.string(), nullable=False),
        pa.field("label", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        pa.field("unit", pa.string(), nullable=False),
        pa.field("value", pa.decimal256(76, 30), nullable=False),
        pa.field("start_date", pa.date32(), nullable=True),
        pa.field("end_date", pa.date32(), nullable=False),
        pa.field("accession_number", pa.string(), nullable=False),
        pa.field("fiscal_year", pa.int32(), nullable=True),
        pa.field("fiscal_period", pa.string(), nullable=True),
        pa.field("form", pa.string(), nullable=False),
        pa.field("filed_date", pa.date32(), nullable=False),
        pa.field("frame", pa.string(), nullable=True),
    ]
)


class SecBronzeValidationError(ValueError):
    """Raised when parsed SEC data cannot form its Bronze Arrow table."""


@dataclass(frozen=True)
class SecBronzeDatasetResult:
    """Locations and count for one persisted SEC endpoint dataset."""

    cik: str
    location: BronzeRunLocation
    raw_json_path: Path
    parquet_path: Path
    record_count: int


@dataclass(frozen=True)
class SecCompanyBronzeResult:
    """Results for both SEC datasets in one company-level ingestion."""

    cik: str
    submissions: SecBronzeDatasetResult
    company_facts: SecBronzeDatasetResult


def sec_submissions_to_table(submissions: SecSubmissions) -> pa.Table:
    """Convert parsed SEC submissions to the stable filing-row schema."""
    if not isinstance(submissions, SecSubmissions):
        raise SecBronzeValidationError("Expected a SecSubmissions value")

    rows: list[dict[str, object]] = []
    for index, filing in enumerate(submissions.filings):
        if not isinstance(filing, SecFilingMetadata):
            raise SecBronzeValidationError(
                f"SEC submissions filing {index} is not a SecFilingMetadata"
            )
        acceptance_datetime = filing.acceptance_datetime
        if acceptance_datetime is not None:
            if (
                acceptance_datetime.tzinfo is None
                or acceptance_datetime.utcoffset() is None
            ):
                raise SecBronzeValidationError(
                    "SEC acceptance datetime must be timezone-aware"
                )
            acceptance_datetime = acceptance_datetime.astimezone(timezone.utc)
        rows.append(
            {
                "cik": submissions.cik,
                "company_name": submissions.company_name,
                "accession_number": filing.accession_number,
                "filing_date": filing.filing_date,
                "report_date": filing.report_date,
                "acceptance_datetime": acceptance_datetime,
                "form": filing.form,
                "act": filing.act,
                "file_number": filing.file_number,
                "film_number": filing.film_number,
                "items": filing.items,
                "size": filing.size,
                "is_xbrl": filing.is_xbrl,
                "is_inline_xbrl": filing.is_inline_xbrl,
                "primary_document": filing.primary_document,
                "primary_doc_description": filing.primary_doc_description,
            }
        )

    return _table_from_rows(
        rows,
        schema=SEC_SUBMISSIONS_SCHEMA,
        dataset_name="submissions",
    )


def sec_company_facts_to_table(company_facts: SecCompanyFacts) -> pa.Table:
    """Convert parsed SEC Company Facts to the stable occurrence-row schema."""
    if not isinstance(company_facts, SecCompanyFacts):
        raise SecBronzeValidationError("Expected a SecCompanyFacts value")

    rows: list[dict[str, object]] = []
    for index, fact in enumerate(company_facts.facts):
        if not isinstance(fact, SecFinancialFact):
            raise SecBronzeValidationError(
                f"SEC Company Facts record {index} is not a SecFinancialFact"
            )
        rows.append(
            {
            "cik": company_facts.cik,
            "entity_name": company_facts.entity_name,
            "taxonomy": fact.taxonomy,
            "concept": fact.concept,
            "label": fact.label,
            "description": fact.description,
            "unit": fact.unit,
            "value": fact.value,
            "start_date": fact.start_date,
            "end_date": fact.end_date,
            "accession_number": fact.accession_number,
            "fiscal_year": fact.fiscal_year,
            "fiscal_period": fact.fiscal_period,
            "form": fact.form,
            "filed_date": fact.filed_date,
            "frame": fact.frame,
            }
        )
    return _table_from_rows(
        rows,
        schema=SEC_COMPANY_FACTS_SCHEMA,
        dataset_name="Company Facts",
    )


def _table_from_rows(
    rows: list[dict[str, object]],
    *,
    schema: pa.Schema,
    dataset_name: str,
) -> pa.Table:
    try:
        table = pa.Table.from_pylist(rows, schema=schema)
        table.validate(full=True)
    except (pa.ArrowException, TypeError, ValueError, OverflowError) as exc:
        raise SecBronzeValidationError(
            f"SEC {dataset_name} values cannot be represented by the Bronze schema"
        ) from exc
    return table

