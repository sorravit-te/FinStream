"""Source-aligned SEC filing metadata models."""

from dataclasses import dataclass
from datetime import date, datetime


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
