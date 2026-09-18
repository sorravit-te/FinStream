"""Load validated SEC Bronze Parquet runs into PostgreSQL source tables."""

from collections.abc import Iterable
from dataclasses import dataclass
import re

import psycopg
import pyarrow as pa

from finstream.bronze.json_storage import raw_json_path
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import parquet_path, read_parquet
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_COMPANY_FACTS_SCHEMA,
    SEC_SUBMISSIONS_BRONZE_DATASET,
    SEC_SUBMISSIONS_SCHEMA,
    SecBronzeDatasetResult,
    SecCompanyBronzeResult,
)


_NORMALIZED_CIK_PATTERN = re.compile(r"[0-9]{10}\Z")

_INSERT_INGESTION_RUN = """
    INSERT INTO source_data.ingestion_runs (
        source,
        dataset,
        run_id,
        ingested_at,
        raw_json_path,
        parquet_path,
        record_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

_INSERT_SEC_SUBMISSION = """
    INSERT INTO source_data.sec_submissions (
        source,
        dataset,
        run_id,
        source_row_number,
        cik,
        company_name,
        accession_number,
        filing_date,
        report_date,
        acceptance_datetime,
        form,
        act,
        file_number,
        film_number,
        items,
        size,
        is_xbrl,
        is_inline_xbrl,
        primary_document,
        primary_doc_description
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_INSERT_SEC_COMPANY_FACT = """
    INSERT INTO source_data.sec_company_facts (
        source,
        dataset,
        run_id,
        source_row_number,
        cik,
        entity_name,
        taxonomy,
        concept,
        label,
        description,
        unit,
        value,
        start_date,
        end_date,
        accession_number,
        fiscal_year,
        fiscal_period,
        form,
        filed_date,
        frame
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


class SecPostgresLoadError(ValueError):
    """Raised when SEC Bronze data violates the loading contract."""


@dataclass(frozen=True)
class SecPostgresLoadResult:
    """Counts produced by loading a complete SEC company Bronze run."""

    submissions_loaded: int
    company_facts_loaded: int


@dataclass(frozen=True)
class _PreparedSecDataset:
    result: SecBronzeDatasetResult
    table: pa.Table


def _validate_cik(cik: object) -> str:
    if (
        not isinstance(cik, str)
        or not _NORMALIZED_CIK_PATTERN.fullmatch(cik)
        or cik == "0000000000"
    ):
        raise SecPostgresLoadError("SEC CIK must be a normalized 10-digit value")
    return cik


def _preflight_dataset(
    result: object,
    *,
    expected_dataset: str,
    expected_schema: pa.Schema,
) -> _PreparedSecDataset:
    """Validate one SEC Bronze dataset completely before any SQL is issued."""
    if type(result) is not SecBronzeDatasetResult:
        raise SecPostgresLoadError("result must be a SecBronzeDatasetResult")
    if not isinstance(result.location, BronzeRunLocation):
        raise SecPostgresLoadError("SEC Bronze location must be a BronzeRunLocation")

    cik = _validate_cik(result.cik)
    metadata = result.location.metadata
    if metadata.source != SEC_BRONZE_SOURCE:
        raise SecPostgresLoadError("SEC Bronze source must be sec_edgar")
    if metadata.dataset != expected_dataset:
        raise SecPostgresLoadError("SEC Bronze dataset does not match this loader")
    if result.raw_json_path != raw_json_path(result.location):
        raise SecPostgresLoadError("SEC Bronze raw JSON path is not canonical")
    if result.parquet_path != parquet_path(result.location):
        raise SecPostgresLoadError("SEC Bronze Parquet path is not canonical")
    if isinstance(result.record_count, bool) or not isinstance(result.record_count, int):
        raise SecPostgresLoadError("SEC Bronze record count must be an integer")
    if result.record_count < 0:
        raise SecPostgresLoadError("SEC Bronze record count must not be negative")

    table = read_parquet(result.location)
    if not table.schema.equals(expected_schema, check_metadata=True):
        raise SecPostgresLoadError("SEC Bronze Parquet schema does not match")
    try:
        table.validate(full=True)
    except pa.ArrowException as exc:
        raise SecPostgresLoadError(
            "SEC Bronze Parquet table failed Arrow validation"
        ) from exc
    if table.num_rows != result.record_count:
        raise SecPostgresLoadError(
            "SEC Bronze record count does not match Parquet row count"
        )
    if any(row_cik != cik for row_cik in table.column("cik").to_pylist()):
        raise SecPostgresLoadError("SEC Bronze Parquet CIK values do not match")
    return _PreparedSecDataset(result=result, table=table)


def _ingestion_run_parameters(
    prepared: _PreparedSecDataset,
) -> tuple[object, ...]:
    result = prepared.result
    metadata = result.location.metadata
    return (
        metadata.source,
        metadata.dataset,
        metadata.run_id,
        metadata.ingested_at,
        str(result.raw_json_path),
        str(result.parquet_path),
        result.record_count,
    )


def _submission_row_parameters(
    prepared: _PreparedSecDataset,
) -> Iterable[tuple[object, ...]]:
    metadata = prepared.result.location.metadata
    for source_row_number, row in enumerate(prepared.table.to_pylist()):
        yield (
            metadata.source,
            metadata.dataset,
            metadata.run_id,
            source_row_number,
            row["cik"],
            row["company_name"],
            row["accession_number"],
            row["filing_date"],
            row["report_date"],
            row["acceptance_datetime"],
            row["form"],
            row["act"],
            row["file_number"],
            row["film_number"],
            row["items"],
            row["size"],
            row["is_xbrl"],
            row["is_inline_xbrl"],
            row["primary_document"],
            row["primary_doc_description"],
        )


def _company_fact_row_parameters(
    prepared: _PreparedSecDataset,
) -> Iterable[tuple[object, ...]]:
    metadata = prepared.result.location.metadata
    for source_row_number, row in enumerate(prepared.table.to_pylist()):
        yield (
            metadata.source,
            metadata.dataset,
            metadata.run_id,
            source_row_number,
            row["cik"],
            row["entity_name"],
            row["taxonomy"],
            row["concept"],
            row["label"],
            row["description"],
            row["unit"],
            row["value"],
            row["start_date"],
            row["end_date"],
            row["accession_number"],
            row["fiscal_year"],
            row["fiscal_period"],
            row["form"],
            row["filed_date"],
            row["frame"],
        )


def _load_prepared_submissions(
    cursor: psycopg.Cursor,
    prepared: _PreparedSecDataset,
) -> None:
    cursor.execute(_INSERT_INGESTION_RUN, _ingestion_run_parameters(prepared))
    if prepared.result.record_count:
        cursor.executemany(_INSERT_SEC_SUBMISSION, _submission_row_parameters(prepared))


def _load_prepared_company_facts(
    cursor: psycopg.Cursor,
    prepared: _PreparedSecDataset,
) -> None:
    cursor.execute(_INSERT_INGESTION_RUN, _ingestion_run_parameters(prepared))
    if prepared.result.record_count:
        cursor.executemany(
            _INSERT_SEC_COMPANY_FACT,
            _company_fact_row_parameters(prepared),
        )


def load_sec_submissions_bronze_to_postgres(
    connection: psycopg.Connection,
    result: SecBronzeDatasetResult,
) -> int:
    """Load one existing SEC submissions Bronze dataset."""
    prepared = _preflight_dataset(
        result,
        expected_dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        expected_schema=SEC_SUBMISSIONS_SCHEMA,
    )
    with connection.cursor() as cursor:
        _load_prepared_submissions(cursor, prepared)
    return prepared.result.record_count


def load_sec_company_facts_bronze_to_postgres(
    connection: psycopg.Connection,
    result: SecBronzeDatasetResult,
) -> int:
    """Load one existing SEC Company Facts Bronze dataset."""
    prepared = _preflight_dataset(
        result,
        expected_dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        expected_schema=SEC_COMPANY_FACTS_SCHEMA,
    )
    with connection.cursor() as cursor:
        _load_prepared_company_facts(cursor, prepared)
    return prepared.result.record_count


def load_sec_company_bronze_to_postgres(
    connection: psycopg.Connection,
    result: SecCompanyBronzeResult,
) -> SecPostgresLoadResult:
    """Load both SEC datasets under one caller-owned transaction boundary."""
    if type(result) is not SecCompanyBronzeResult:
        raise SecPostgresLoadError("result must be a SecCompanyBronzeResult")
    cik = _validate_cik(result.cik)
    submissions = _preflight_dataset(
        result.submissions,
        expected_dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        expected_schema=SEC_SUBMISSIONS_SCHEMA,
    )
    company_facts = _preflight_dataset(
        result.company_facts,
        expected_dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        expected_schema=SEC_COMPANY_FACTS_SCHEMA,
    )
    if submissions.result.cik != cik or company_facts.result.cik != cik:
        raise SecPostgresLoadError("SEC combined Bronze CIK values do not match")

    submissions_metadata = submissions.result.location.metadata
    company_facts_metadata = company_facts.result.location.metadata
    if (
        submissions_metadata.run_id != company_facts_metadata.run_id
        or submissions_metadata.ingested_at != company_facts_metadata.ingested_at
    ):
        raise SecPostgresLoadError("SEC combined Bronze run identity does not match")

    with connection.cursor() as cursor:
        _load_prepared_submissions(cursor, submissions)
        _load_prepared_company_facts(cursor, company_facts)

    return SecPostgresLoadResult(
        submissions_loaded=submissions.result.record_count,
        company_facts_loaded=company_facts.result.record_count,
    )
