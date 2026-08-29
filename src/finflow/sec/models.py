"""Source-aligned SEC filing metadata models."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


@dataclass(frozen=True)
class SecFilingMetadata:
    """Metadata for one filing from an SEC submissions response."""

    cik: str
    accession_number: str
    filing_date: date
    report_date: date | None
    acceptance_datetime: datetime | None
    form: str
    act: str | None
    file_number: str | None
    film_number: str | None
    items: str | None
    size: int
    is_xbrl: bool
    is_inline_xbrl: bool
    primary_document: str | None
    primary_doc_description: str | None


@dataclass(frozen=True)
class SecSubmissions:
    """Parsed recent filing metadata for one SEC company."""

    cik: str
    company_name: str
    filings: tuple[SecFilingMetadata, ...]


@dataclass(frozen=True)
class SecFinancialFact:
    """One source-aligned occurrence from an SEC Company Facts response."""

    cik: str
    taxonomy: str
    concept: str
    label: str | None
    description: str | None
    unit: str
    value: Decimal
    start_date: date | None
    end_date: date
    accession_number: str
    fiscal_year: int | None
    fiscal_period: str | None
    form: str
    filed_date: date
    frame: str | None


@dataclass(frozen=True)
class SecCompanyFacts:
    """Parsed source-aligned financial facts for one SEC company."""

    cik: str
    entity_name: str
    facts: tuple[SecFinancialFact, ...]


@dataclass(frozen=True)
class SecCompanySourceData:
    """Successfully parsed SEC source data for one company."""

    cik: str
    submissions: SecSubmissions
    company_facts: SecCompanyFacts
