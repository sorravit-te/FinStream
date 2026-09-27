"""Airflow-independent, read-only quality monitoring runtime boundary."""

from datetime import date, datetime

from finstream.config import Settings, load_settings
from finstream.database.connection import connect_postgres
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
)
from finstream.fred.series import INITIAL_FRED_SERIES_IDS
from finstream.market.bronze import MARKET_BRONZE_DATASET, MARKET_BRONZE_SOURCE
from finstream.quality.monitoring import (
    QualitySignal,
    QualityStatus,
    monitor_fred_observation_freshness,
    monitor_market_freshness,
    monitor_run_count_movement,
    monitor_sec_company_facts_freshness,
    monitor_sec_submissions_freshness,
)
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_SUBMISSIONS_BRONZE_DATASET,
)
from finstream.sec.companies import INITIAL_SEC_COMPANIES


QualityMonitoringResult = dict[str, object]

_RUN_COUNT_DATASETS = (
    (MARKET_BRONZE_SOURCE, MARKET_BRONZE_DATASET),
    (SEC_BRONZE_SOURCE, SEC_SUBMISSIONS_BRONZE_DATASET),
    (SEC_BRONZE_SOURCE, SEC_COMPANY_FACTS_BRONZE_DATASET),
    (FRED_BRONZE_SOURCE, FRED_SERIES_METADATA_BRONZE_DATASET),
    (FRED_BRONZE_SOURCE, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET),
)


def run_quality_monitoring(
    *,
    as_of: date,
    settings: Settings | None = None,
) -> QualityMonitoringResult:
    """Evaluate approved read-only monitoring signals for configured entities."""
    _validate_as_of(as_of)
    active_settings = settings if settings is not None else load_settings()
    with connect_postgres(active_settings.postgres_dsn) as connection:
        signals = collect_quality_signals(connection, as_of=as_of)
    return _serialize_monitoring_result(as_of=as_of, signals=signals)


def collect_quality_signals(
    connection: object,
    *,
    as_of: date,
) -> list[QualitySignal]:
    """Collect the approved read-only quality signals on one open connection."""
    _validate_as_of(as_of)
    signals: list[QualitySignal] = []
    for company in INITIAL_SEC_COMPANIES:
        signals.append(monitor_market_freshness(connection, company.ticker, as_of=as_of))
    for series_id in INITIAL_FRED_SERIES_IDS:
        signals.append(monitor_fred_observation_freshness(connection, series_id, as_of=as_of))
    for company in INITIAL_SEC_COMPANIES:
        signals.append(monitor_sec_submissions_freshness(connection, company.cik, as_of=as_of))
        signals.append(
            monitor_sec_company_facts_freshness(connection, company.cik, as_of=as_of)
        )
    for source, dataset in _RUN_COUNT_DATASETS:
        signals.append(
            monitor_run_count_movement(
                connection,
                source=source,
                dataset=dataset,
                as_of=as_of,
            )
        )
    return signals


def _serialize_monitoring_result(
    *,
    as_of: date,
    signals: list[QualitySignal],
) -> QualityMonitoringResult:
    status_counts = {status.value: 0 for status in QualityStatus}
    serialized_signals = []
    for signal in signals:
        status_counts[signal.status.value] += 1
        serialized_signals.append(
            {
                "check_id": signal.check_id,
                "source": signal.source,
                "dataset": signal.dataset,
                "entity_id": signal.entity_id,
                "dimension": signal.dimension,
                "status": signal.status.value,
                "as_of": signal.as_of.isoformat(),
                "measurements": [
                    {"name": measurement.name, "value": _serialize_value(measurement.value)}
                    for measurement in signal.measurements
                ],
                "message": signal.message,
            }
        )
    return {
        "as_of": as_of.isoformat(),
        "signal_count": len(serialized_signals),
        "status_counts": status_counts,
        "signals": serialized_signals,
    }


def _serialize_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _validate_as_of(as_of: object) -> None:
    if isinstance(as_of, datetime) or not isinstance(as_of, date):
        raise ValueError("as_of must be a date")
