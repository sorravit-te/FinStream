"""Source-aware, read-only quality measurements for loaded source data."""

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum

import psycopg

from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
)
from finstream.fred.postgres import (
    FredPostgresLoadError,
    latest_fred_observation_date,
)
from finstream.market.bronze import (
    MARKET_BRONZE_DATASET,
    MARKET_BRONZE_SOURCE,
)
from finstream.market.postgres import (
    MarketPostgresLoadError,
    latest_market_trading_date,
)
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_SUBMISSIONS_BRONZE_DATASET,
)
from finstream.sec.edgar import _normalize_cik


_SELECT_LATEST_SUBMISSIONS_FILED_DATE = """
    SELECT max(filing_date)
    FROM source_data.sec_submissions
    WHERE source = %s AND dataset = %s AND cik = %s
"""

_SELECT_LATEST_COMPANY_FACTS_FILED_DATE = """
    SELECT max(filed_date)
    FROM source_data.sec_company_facts
    WHERE source = %s AND dataset = %s AND cik = %s
"""

_SELECT_LATEST_RUN_COUNTS = """
    SELECT record_count, ingested_at, run_id
    FROM source_data.ingestion_runs
    WHERE source = %s AND dataset = %s
    ORDER BY ingested_at DESC, run_id DESC
    LIMIT 2
"""


class QualityStatus(str, Enum):
    """Closed statuses for non-mutating quality signals."""

    PASS = "pass"
    WARNING = "warning"
    INFO = "info"
    ERROR = "error"


@dataclass(frozen=True)
class QualityMeasurement:
    """One observed scalar retained in a quality signal."""

    name: str
    value: date | datetime | int | float | str | bool | None


@dataclass(frozen=True)
class QualitySignal:
    """A deterministic, locally consumable monitoring result."""

    check_id: str
    source: str
    dataset: str
    entity_id: str | None
    dimension: str
    status: QualityStatus
    as_of: date
    measurements: tuple[QualityMeasurement, ...]
    message: str


def monitor_market_freshness(
    connection: psycopg.Connection,
    symbol: str,
    *,
    as_of: date,
) -> QualitySignal:
    """Measure Market recency without applying an exchange-calendar policy."""
    as_of = _validate_as_of(as_of)
    entity_id = symbol.strip().upper() if isinstance(symbol, str) else None
    try:
        latest_date = latest_market_trading_date(connection, symbol)
    except (MarketPostgresLoadError, psycopg.Error) as exc:
        return _error_signal(
            check_id="market_latest_trading_date",
            source=MARKET_BRONZE_SOURCE,
            dataset=MARKET_BRONZE_DATASET,
            entity_id=entity_id,
            dimension="freshness",
            as_of=as_of,
            message=f"Could not measure Market recency: {exc}",
        )
    return _freshness_signal(
        check_id="market_latest_trading_date",
        source=MARKET_BRONZE_SOURCE,
        dataset=MARKET_BRONZE_DATASET,
        entity_id=entity_id,
        latest_date=latest_date,
        as_of=as_of,
        no_data_message="No loaded Market observations for this symbol.",
        measured_message=(
            "Market calendar-day age is informational because no exchange "
            "calendar or freshness threshold is configured."
        ),
    )


def monitor_fred_observation_freshness(
    connection: psycopg.Connection,
    series_id: str,
    *,
    as_of: date,
) -> QualitySignal:
    """Measure FRED observation recency without inferring a cadence policy."""
    as_of = _validate_as_of(as_of)
    entity_id = series_id.strip() if isinstance(series_id, str) else None
    try:
        latest_date = latest_fred_observation_date(connection, series_id)
    except (FredPostgresLoadError, psycopg.Error) as exc:
        return _error_signal(
            check_id="fred_latest_observation_date",
            source=FRED_BRONZE_SOURCE,
            dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
            entity_id=entity_id,
            dimension="freshness",
            as_of=as_of,
            message=f"Could not measure FRED observation recency: {exc}",
        )
    return _freshness_signal(
        check_id="fred_latest_observation_date",
        source=FRED_BRONZE_SOURCE,
        dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
        entity_id=entity_id,
        latest_date=latest_date,
        as_of=as_of,
        no_data_message="No loaded FRED observations for this series.",
        measured_message=(
            "FRED calendar-day age is informational because no source-specific "
            "cadence threshold is configured."
        ),
    )


def monitor_sec_submissions_freshness(
    connection: psycopg.Connection,
    cik: str | int,
    *,
    as_of: date,
) -> QualitySignal:
    """Measure SEC submissions filing recency without a filing-frequency SLA."""
    return _monitor_sec_filed_date(
        connection,
        cik=cik,
        as_of=as_of,
        check_id="sec_latest_submissions_filing_date",
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        sql=_SELECT_LATEST_SUBMISSIONS_FILED_DATE,
        no_data_message="No loaded SEC submissions for this CIK.",
    )


def monitor_sec_company_facts_freshness(
    connection: psycopg.Connection,
    cik: str | int,
    *,
    as_of: date,
) -> QualitySignal:
    """Measure SEC Company Facts filing recency without a filing-frequency SLA."""
    return _monitor_sec_filed_date(
        connection,
        cik=cik,
        as_of=as_of,
        check_id="sec_latest_company_facts_filed_date",
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        sql=_SELECT_LATEST_COMPANY_FACTS_FILED_DATE,
        no_data_message="No loaded SEC Company Facts for this CIK.",
    )


def monitor_run_count_movement(
    connection: psycopg.Connection,
    *,
    source: str,
    dataset: str,
    as_of: date,
) -> QualitySignal:
    """Measure consecutive run counts without treating them as comparable scope."""
    as_of = _validate_as_of(as_of)
    if not isinstance(source, str) or not source.strip():
        return _error_signal(
            check_id="ingestion_run_count_movement",
            source="",
            dataset=dataset if isinstance(dataset, str) else "",
            entity_id=None,
            dimension="record_count",
            as_of=as_of,
            message="Could not measure run counts: source must not be blank.",
        )
    if not isinstance(dataset, str) or not dataset.strip():
        return _error_signal(
            check_id="ingestion_run_count_movement",
            source=source.strip(),
            dataset="",
            entity_id=None,
            dimension="record_count",
            as_of=as_of,
            message="Could not measure run counts: dataset must not be blank.",
        )

    normalized_source = source.strip()
    normalized_dataset = dataset.strip()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                _SELECT_LATEST_RUN_COUNTS,
                (normalized_source, normalized_dataset),
            )
            rows = cursor.fetchall()
        runs = tuple(_validate_run_row(row) for row in rows)
    except (MonitoringExecutionError, psycopg.Error) as exc:
        return _error_signal(
            check_id="ingestion_run_count_movement",
            source=normalized_source,
            dataset=normalized_dataset,
            entity_id=None,
            dimension="record_count",
            as_of=as_of,
            message=f"Could not measure run counts: {exc}",
        )

    if not runs:
        return QualitySignal(
            check_id="ingestion_run_count_movement",
            source=normalized_source,
            dataset=normalized_dataset,
            entity_id=None,
            dimension="record_count",
            status=QualityStatus.INFO,
            as_of=as_of,
            measurements=(
                QualityMeasurement("current_record_count", None),
                QualityMeasurement("previous_record_count", None),
                QualityMeasurement("scope_comparable", False),
            ),
            message="No loaded ingestion runs are available for this dataset.",
        )

    current_count, current_ingested_at, current_run_id = runs[0]
    previous = runs[1] if len(runs) > 1 else None
    previous_count = previous[0] if previous is not None else None
    absolute_delta = (
        current_count - previous_count if previous_count is not None else None
    )
    percent_delta = (
        (current_count - previous_count) / previous_count * 100
        if previous_count not in (None, 0)
        else None
    )
    measurements = [
        QualityMeasurement("current_record_count", current_count),
        QualityMeasurement("current_ingested_at", current_ingested_at),
        QualityMeasurement("current_run_id", current_run_id),
        QualityMeasurement("previous_record_count", previous_count),
        QualityMeasurement("absolute_delta", absolute_delta),
        QualityMeasurement("percent_delta", percent_delta),
        QualityMeasurement("scope_comparable", False),
    ]
    if previous is not None:
        measurements.extend(
            [
                QualityMeasurement("previous_ingested_at", previous[1]),
                QualityMeasurement("previous_run_id", previous[2]),
            ]
        )
    message = (
        "Run counts are informational because retrieval scope comparability is "
        "not recorded."
    )
    if previous_count == 0:
        message = (
            "Run counts are informational because retrieval scope comparability "
            "is not recorded; percentage movement from zero is undefined."
        )
    return QualitySignal(
        check_id="ingestion_run_count_movement",
        source=normalized_source,
        dataset=normalized_dataset,
        entity_id=None,
        dimension="record_count",
        status=QualityStatus.INFO,
        as_of=as_of,
        measurements=tuple(measurements),
        message=message,
    )


class MonitoringExecutionError(ValueError):
    """Raised internally when PostgreSQL monitoring state is malformed."""


def _monitor_sec_filed_date(
    connection: psycopg.Connection,
    *,
    cik: str | int,
    as_of: date,
    check_id: str,
    dataset: str,
    sql: str,
    no_data_message: str,
) -> QualitySignal:
    as_of = _validate_as_of(as_of)
    try:
        normalized_cik = _normalize_cik(cik)
        with connection.cursor() as cursor:
            cursor.execute(sql, (SEC_BRONZE_SOURCE, dataset, normalized_cik))
            row = cursor.fetchone()
        if row is None or len(row) != 1:
            raise MonitoringExecutionError(
                "SEC filed-date query returned an unexpected row shape"
            )
        latest_date = row[0]
        if latest_date is not None and (
            isinstance(latest_date, datetime) or not isinstance(latest_date, date)
        ):
            raise MonitoringExecutionError(
                "SEC filed-date query returned an invalid date"
            )
    except (MonitoringExecutionError, TypeError, ValueError, psycopg.Error) as exc:
        return _error_signal(
            check_id=check_id,
            source=SEC_BRONZE_SOURCE,
            dataset=dataset,
            entity_id=None,
            dimension="freshness",
            as_of=as_of,
            message=f"Could not measure SEC filing recency: {exc}",
        )

    return _freshness_signal(
        check_id=check_id,
        source=SEC_BRONZE_SOURCE,
        dataset=dataset,
        entity_id=normalized_cik,
        latest_date=latest_date,
        as_of=as_of,
        no_data_message=no_data_message,
        measured_message=(
            "SEC filing calendar-day age is informational because filings are "
            "event-driven and no freshness SLA is configured."
        ),
    )


def _freshness_signal(
    *,
    check_id: str,
    source: str,
    dataset: str,
    entity_id: str | None,
    latest_date: date | None,
    as_of: date,
    no_data_message: str,
    measured_message: str,
) -> QualitySignal:
    age_calendar_days = (as_of - latest_date).days if latest_date is not None else None
    return QualitySignal(
        check_id=check_id,
        source=source,
        dataset=dataset,
        entity_id=entity_id,
        dimension="freshness",
        status=QualityStatus.INFO,
        as_of=as_of,
        measurements=(
            QualityMeasurement("latest_date", latest_date),
            QualityMeasurement("age_calendar_days", age_calendar_days),
        ),
        message=measured_message if latest_date is not None else no_data_message,
    )


def _error_signal(
    *,
    check_id: str,
    source: str,
    dataset: str,
    entity_id: str | None,
    dimension: str,
    as_of: date,
    message: str,
) -> QualitySignal:
    return QualitySignal(
        check_id=check_id,
        source=source,
        dataset=dataset,
        entity_id=entity_id,
        dimension=dimension,
        status=QualityStatus.ERROR,
        as_of=as_of,
        measurements=(),
        message=message,
    )


def _validate_as_of(value: object) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError("as_of must be a date")
    return value


def _validate_run_row(row: object) -> tuple[int, datetime, str]:
    if not isinstance(row, tuple) or len(row) != 3:
        raise MonitoringExecutionError("Ingestion-run query returned an invalid row")
    record_count, ingested_at, run_id = row
    if isinstance(record_count, bool) or not isinstance(record_count, int):
        raise MonitoringExecutionError("Ingestion-run record_count is invalid")
    if record_count < 0:
        raise MonitoringExecutionError("Ingestion-run record_count is invalid")
    if not isinstance(ingested_at, datetime):
        raise MonitoringExecutionError("Ingestion-run ingested_at is invalid")
    if not isinstance(run_id, str) or not run_id:
        raise MonitoringExecutionError("Ingestion-run run_id is invalid")
    return record_count, ingested_at, run_id
