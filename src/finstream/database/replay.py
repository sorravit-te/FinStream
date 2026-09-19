"""Exact-run replay verification for source-aligned PostgreSQL loaders."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import psycopg


_SOURCE_TABLES = frozenset(
    {
        "source_data.market_daily_prices",
        "source_data.sec_submissions",
        "source_data.sec_company_facts",
        "source_data.fred_series_metadata",
        "source_data.fred_series_observations",
    }
)

_SELECT_INGESTION_RUN = """
    SELECT
        source,
        dataset,
        run_id,
        ingested_at,
        raw_json_path,
        parquet_path,
        record_count
    FROM source_data.ingestion_runs
    WHERE source = %s AND dataset = %s AND run_id = %s
"""


class IngestionRunReplayError(ValueError):
    """Raised when an existing ingestion run cannot be verified as a replay."""


class IngestionRunState(Enum):
    """State of a validated ingestion run in the current transaction."""

    ABSENT = "absent"
    INSERTED = "inserted"
    VERIFIED_REPLAY = "verified_replay"


@dataclass(frozen=True)
class IngestionRun:
    """Registry metadata and source table for one validated Bronze dataset."""

    source: str
    dataset: str
    run_id: str
    ingested_at: datetime
    raw_json_path: str
    parquet_path: str
    record_count: int
    source_table: str

    @property
    def registry_parameters(self) -> tuple[object, ...]:
        """Return parameters in the source_data.ingestion_runs column order."""
        return (
            self.source,
            self.dataset,
            self.run_id,
            self.ingested_at,
            self.raw_json_path,
            self.parquet_path,
            self.record_count,
        )


def inspect_ingestion_run(
    cursor: psycopg.Cursor,
    *,
    run: IngestionRun,
) -> IngestionRunState:
    """Verify an existing run, or report that its registry row is absent."""
    _validate_source_table(run.source_table)
    cursor.execute(
        _SELECT_INGESTION_RUN,
        (run.source, run.dataset, run.run_id),
    )
    stored = cursor.fetchone()
    if stored is None:
        return IngestionRunState.ABSENT

    _verify_registry_metadata(run, stored)
    cursor.execute(
        _source_row_count_sql(run.source_table),
        (run.source, run.dataset, run.run_id),
    )
    count_row = cursor.fetchone()
    if count_row is None:
        raise IngestionRunReplayError(
            "Existing ingestion run source-row count query returned no row"
        )
    source_row_count = count_row[0]
    if source_row_count != run.record_count:
        raise IngestionRunReplayError(
            "Existing ingestion run source-row count does not match record_count"
        )
    return IngestionRunState.VERIFIED_REPLAY


def register_or_verify_ingestion_run(
    cursor: psycopg.Cursor,
    *,
    run: IngestionRun,
    insert_sql: str,
) -> IngestionRunState:
    """Register a new run or verify an exact committed replay.

    ``insert_sql`` must insert into ``source_data.ingestion_runs`` with an
    exact-run ``ON CONFLICT DO NOTHING RETURNING`` clause. The registry primary
    key remains the final authority when a concurrent caller races to load the
    same run.
    """
    _validate_source_table(run.source_table)
    cursor.execute(insert_sql, run.registry_parameters)
    if cursor.fetchone() is not None:
        return IngestionRunState.INSERTED
    return inspect_ingestion_run(cursor, run=run)


def _validate_source_table(source_table: str) -> None:
    if source_table not in _SOURCE_TABLES:
        raise ValueError("Unsupported source table for ingestion-run replay")


def _verify_registry_metadata(
    run: IngestionRun,
    stored: tuple[object, ...],
) -> None:
    expected = run.registry_parameters
    field_names = (
        "source",
        "dataset",
        "run_id",
        "ingested_at",
        "raw_json_path",
        "parquet_path",
        "record_count",
    )
    if len(stored) != len(expected):
        raise IngestionRunReplayError(
            "Existing ingestion run metadata has an unexpected shape"
        )
    mismatched = [
        field_name
        for field_name, expected_value, stored_value in zip(
            field_names,
            expected,
            stored,
            strict=True,
        )
        if expected_value != stored_value
    ]
    if mismatched:
        raise IngestionRunReplayError(
            "Existing ingestion run metadata does not match supplied Bronze "
            f"result: {', '.join(mismatched)}"
        )


def _source_row_count_sql(source_table: str) -> str:
    return f"""
        SELECT count(*)
        FROM {source_table}
        WHERE source = %s AND dataset = %s AND run_id = %s
    """
