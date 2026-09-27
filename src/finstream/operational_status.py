"""Read-only V1 operational status checks for local FinStream operators."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import date, datetime
from enum import Enum
import json
import os
from pathlib import Path
import re
import sys
from typing import TextIO

import psycopg

from finstream.bronze.json_storage import RAW_JSON_FILENAME
from finstream.bronze.parquet_storage import STRUCTURED_PARQUET_FILENAME
from finstream.bronze.paths import DEFAULT_BRONZE_ROOT
from finstream.config import Settings, load_settings
from finstream.database.connection import connect_postgres
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
)
from finstream.market.bronze import MARKET_BRONZE_DATASET, MARKET_BRONZE_SOURCE
from finstream.orchestration.quality_runtime import collect_quality_signals
from finstream.quality.monitoring import QualitySignal, QualityStatus
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_SUBMISSIONS_BRONZE_DATASET,
)


_KNOWN_DATASETS = (
    (MARKET_BRONZE_SOURCE, MARKET_BRONZE_DATASET),
    (SEC_BRONZE_SOURCE, SEC_SUBMISSIONS_BRONZE_DATASET),
    (SEC_BRONZE_SOURCE, SEC_COMPANY_FACTS_BRONZE_DATASET),
    (FRED_BRONZE_SOURCE, FRED_SERIES_METADATA_BRONZE_DATASET),
    (FRED_BRONZE_SOURCE, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET),
)
_EXPECTED_ANALYTICS_RELATIONS = (
    "analytics.mart_company_daily_performance",
    "analytics.mart_company_financial_growth",
    "analytics.mart_market_macro",
)
_SELECT_DATABASE_IDENTITY = "SELECT current_database(), current_user"
_SELECT_LATEST_PROVENANCE = """
    SELECT DISTINCT ON (source, dataset)
        source, dataset, run_id, ingested_at, record_count
    FROM source_data.ingestion_runs
    WHERE (source, dataset) IN ((%s, %s), (%s, %s), (%s, %s), (%s, %s), (%s, %s))
    ORDER BY source, dataset, ingested_at DESC, run_id DESC
"""
_SELECT_RELATION = "SELECT to_regclass(%s)"
_POSTGRES_URL_PASSWORD = re.compile(r"(postgres(?:ql)?://[^:/\s]+:)([^@\s]+)(@)", re.I)
_CONNINFO_PASSWORD = re.compile(r"(password\s*=\s*)(?:'[^']*'|\"[^\"]*\"|\S+)", re.I)


class OperationalStatus(str, Enum):
    """Closed status values for deterministic operational checks."""

    OK = "ok"
    INFO = "info"
    ERROR = "error"


@dataclass(frozen=True)
class OperationalDetail:
    """One scalar reported by an operational check."""

    name: str
    value: date | datetime | int | str | bool | None


@dataclass(frozen=True)
class OperationalCheck:
    """A concise, non-mutating operational result."""

    check_id: str
    status: OperationalStatus
    message: str
    details: tuple[OperationalDetail, ...]


@dataclass(frozen=True)
class OperationalStatusReport:
    """The stable result produced by the operational-status entry point."""

    as_of: date
    checks: tuple[OperationalCheck, ...]

    @property
    def status(self) -> OperationalStatus:
        if any(check.status is OperationalStatus.ERROR for check in self.checks):
            return OperationalStatus.ERROR
        if any(check.status is OperationalStatus.INFO for check in self.checks):
            return OperationalStatus.INFO
        return OperationalStatus.OK


def collect_operational_status(
    *,
    as_of: date,
    settings: Settings | None = None,
    bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
) -> OperationalStatusReport:
    """Collect database, Bronze, provenance, analytics, and recency status.

    This function only reads the filesystem and issues PostgreSQL ``SELECT``
    statements. ``as_of`` is required so recency remains an explicit
    measurement rather than an implicit freshness policy.
    """
    _validate_as_of(as_of)
    active_settings = settings if settings is not None else load_settings()
    checks = [_bronze_check(Path(bronze_root))]
    secrets = _settings_secret_values(active_settings)

    try:
        with connect_postgres(active_settings.postgres_dsn) as connection:
            checks.insert(0, _database_check(connection))
            checks.append(_provenance_check(connection))
            checks.append(_analytics_check(connection))
            checks.append(_recency_check(connection, as_of=as_of))
    except (Exception,) as exc:
        checks.insert(
            0,
            OperationalCheck(
                check_id="postgresql",
                status=OperationalStatus.ERROR,
                message=f"PostgreSQL operational evaluation failed: {_redact_text(str(exc), secrets)}",
                details=(),
            ),
        )

    return OperationalStatusReport(as_of=as_of, checks=tuple(checks))


def serialize_operational_status(report: OperationalStatusReport) -> dict[str, object]:
    """Serialize a status report into the documented stable JSON shape."""
    return {
        "as_of": report.as_of.isoformat(),
        "status": report.status.value,
        "checks": [
            {
                "check_id": check.check_id,
                "status": check.status.value,
                "message": check.message,
                "details": [
                    {"name": detail.name, "value": _serialize_value(detail.value)}
                    for detail in check.details
                ],
            }
            for check in report.checks
        ],
    }


def redact_operational_output(value: str, *, secrets: tuple[str | None, ...] = ()) -> str:
    """Redact PostgreSQL password syntax and supplied secret values from output."""
    redacted = _POSTGRES_URL_PASSWORD.sub(r"\1***\3", value)
    redacted = _CONNINFO_PASSWORD.sub(r"\1***", redacted)
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "***")
    return redacted


def main(argv: list[str] | None = None, *, stdout: TextIO | None = None) -> int:
    """Run the command-line status interface and return its deterministic code."""
    parser = argparse.ArgumentParser(description="Report read-only FinStream operational status.")
    parser.add_argument(
        "--as-of",
        required=True,
        type=_parse_as_of,
        help="ISO date used only for explicit informational recency measurement.",
    )
    parser.add_argument(
        "--bronze-root",
        default=str(DEFAULT_BRONZE_ROOT),
        help="Local Bronze root to inspect without recursive scanning (default: data/bronze).",
    )
    parser.add_argument("--json", action="store_true", help="Emit stable JSON output.")
    parser.add_argument(
        "--test-dsn",
        action="store_true",
        help="Use FINSTREAM_TEST_POSTGRES_DSN for a disposable integration database.",
    )
    args = parser.parse_args(argv)
    active_settings = load_settings()
    if args.test_dsn:
        active_settings = replace(
            active_settings,
            postgres_dsn=os.environ.get("FINSTREAM_TEST_POSTGRES_DSN"),
        )
    report = collect_operational_status(
        as_of=args.as_of,
        settings=active_settings,
        bronze_root=args.bronze_root,
    )
    output = stdout if stdout is not None else sys.stdout
    secrets = _settings_secret_values(active_settings)
    if args.json:
        rendered = json.dumps(serialize_operational_status(report), sort_keys=True)
    else:
        rendered = _format_human_report(report)
    print(redact_operational_output(rendered, secrets=secrets), file=output)
    return 1 if report.status is OperationalStatus.ERROR else 0


def _database_check(connection: psycopg.Connection) -> OperationalCheck:
    try:
        with connection.cursor() as cursor:
            cursor.execute(_SELECT_DATABASE_IDENTITY)
            row = cursor.fetchone()
        if not isinstance(row, tuple) or len(row) != 2:
            raise ValueError("database identity query returned an invalid row")
        database_name, database_user = row
        if not isinstance(database_name, str) or not database_name:
            raise ValueError("database identity is invalid")
        if not isinstance(database_user, str) or not database_user:
            raise ValueError("database identity is invalid")
    except Exception as exc:
        return OperationalCheck(
            check_id="postgresql",
            status=OperationalStatus.ERROR,
            message=f"PostgreSQL connectivity check failed: {_redact_text(str(exc), ())}",
            details=(),
        )
    return OperationalCheck(
        check_id="postgresql",
        status=OperationalStatus.OK,
        message="PostgreSQL is reachable.",
        details=(
            OperationalDetail("database", database_name),
            OperationalDetail("user", database_user),
        ),
    )


def _provenance_check(connection: psycopg.Connection) -> OperationalCheck:
    try:
        with connection.cursor() as cursor:
            cursor.execute(_SELECT_LATEST_PROVENANCE, tuple(item for pair in _KNOWN_DATASETS for item in pair))
            rows = cursor.fetchall()
        latest = _validated_provenance_rows(rows)
    except Exception as exc:
        return OperationalCheck(
            check_id="provenance",
            status=OperationalStatus.ERROR,
            message=f"Provenance check failed: {_redact_text(str(exc), ())}",
            details=(),
        )

    details: list[OperationalDetail] = []
    for source, dataset in _KNOWN_DATASETS:
        value = latest.get((source, dataset))
        prefix = f"{source}.{dataset}"
        if value is None:
            details.append(OperationalDetail(f"{prefix}.latest_run_id", None))
            continue
        run_id, ingested_at, record_count = value
        details.extend(
            (
                OperationalDetail(f"{prefix}.latest_run_id", run_id),
                OperationalDetail(f"{prefix}.latest_recorded_at", ingested_at),
                OperationalDetail(f"{prefix}.latest_record_count", record_count),
            )
        )
    return OperationalCheck(
        check_id="provenance",
        status=OperationalStatus.INFO,
        message="Latest ingestion provenance is reported where loaded; empty datasets are valid.",
        details=tuple(details),
    )


def _analytics_check(connection: psycopg.Connection) -> OperationalCheck:
    details: list[OperationalDetail] = []
    missing: list[str] = []
    try:
        with connection.cursor() as cursor:
            for relation in _EXPECTED_ANALYTICS_RELATIONS:
                cursor.execute(_SELECT_RELATION, (relation,))
                row = cursor.fetchone()
                if not isinstance(row, tuple) or len(row) != 1:
                    raise ValueError("analytics relation query returned an invalid row")
                exists = row[0] is not None
                details.append(OperationalDetail(relation, exists))
                if not exists:
                    missing.append(relation)
    except Exception as exc:
        return OperationalCheck(
            check_id="analytics_relations",
            status=OperationalStatus.ERROR,
            message=f"Analytics relation check failed: {_redact_text(str(exc), ())}",
            details=tuple(details),
        )
    if missing:
        return OperationalCheck(
            check_id="analytics_relations",
            status=OperationalStatus.ERROR,
            message=f"Expected analytics relations are missing: {', '.join(missing)}.",
            details=tuple(details),
        )
    return OperationalCheck(
        check_id="analytics_relations",
        status=OperationalStatus.OK,
        message="Expected analytics relations are available.",
        details=tuple(details),
    )


def _recency_check(connection: psycopg.Connection, *, as_of: date) -> OperationalCheck:
    try:
        signals = collect_quality_signals(connection, as_of=as_of)
    except Exception as exc:
        return OperationalCheck(
            check_id="recency",
            status=OperationalStatus.ERROR,
            message=f"Recency evaluation failed: {_redact_text(str(exc), ())}",
            details=(),
        )

    details: list[OperationalDetail] = []
    errors = 0
    for signal in signals:
        if signal.dimension != "freshness":
            continue
        if signal.status is QualityStatus.ERROR:
            errors += 1
        latest_date = _signal_measurement(signal, "latest_date")
        age = _signal_measurement(signal, "age_calendar_days")
        label = signal.check_id if signal.entity_id is None else f"{signal.check_id}.{signal.entity_id}"
        details.extend(
            (
                OperationalDetail(f"{label}.status", signal.status.value),
                OperationalDetail(f"{label}.latest_date", latest_date),
                OperationalDetail(f"{label}.age_calendar_days", age),
            )
        )
    if errors:
        return OperationalCheck(
            check_id="recency",
            status=OperationalStatus.ERROR,
            message=f"{errors} existing recency measurement(s) could not be evaluated.",
            details=tuple(details),
        )
    return OperationalCheck(
        check_id="recency",
        status=OperationalStatus.INFO,
        message="Recency measurements are informational; no freshness threshold is applied.",
        details=tuple(details),
    )


def _bronze_check(bronze_root: Path) -> OperationalCheck:
    details: list[OperationalDetail] = []
    incomplete: list[str] = []
    try:
        for source, dataset in _KNOWN_DATASETS:
            location = _latest_bronze_run_directory(bronze_root / source / dataset)
            prefix = f"{source}.{dataset}"
            if location is None:
                details.append(OperationalDetail(f"{prefix}.latest_run_directory", None))
                continue
            raw_exists = (location / RAW_JSON_FILENAME).is_file()
            parquet_exists = (location / STRUCTURED_PARQUET_FILENAME).is_file()
            pair_present = raw_exists and parquet_exists
            details.extend(
                (
                    OperationalDetail(f"{prefix}.latest_run_directory", str(location)),
                    OperationalDetail(f"{prefix}.latest_artifact_pair_present", pair_present),
                )
            )
            if not pair_present:
                incomplete.append(str(location))
    except OSError as exc:
        return OperationalCheck(
            check_id="bronze",
            status=OperationalStatus.ERROR,
            message=f"Bronze filesystem check failed: {_redact_text(str(exc), ())}",
            details=tuple(details),
        )
    if incomplete:
        return OperationalCheck(
            check_id="bronze",
            status=OperationalStatus.ERROR,
            message="A latest Bronze run is missing its required Raw JSON or Parquet file.",
            details=tuple(details),
        )
    return OperationalCheck(
        check_id="bronze",
        status=OperationalStatus.INFO,
        message="Latest Bronze run directories are inspected without recursive scans; absent local data is informational.",
        details=tuple(details),
    )


def _latest_bronze_run_directory(dataset_directory: Path) -> Path | None:
    if not dataset_directory.is_dir():
        return None
    date_directories = [
        path
        for path in dataset_directory.iterdir()
        if path.is_dir() and path.name.startswith("ingestion_date=")
    ]
    if not date_directories:
        return None
    latest_date_directory = max(date_directories, key=lambda path: path.name)
    run_directories = [
        path
        for path in latest_date_directory.iterdir()
        if path.is_dir() and path.name.startswith("run_id=")
    ]
    if not run_directories:
        return None
    return max(run_directories, key=lambda path: path.name)


def _validated_provenance_rows(
    rows: object,
) -> dict[tuple[str, str], tuple[str, datetime, int]]:
    if not isinstance(rows, list):
        raise ValueError("provenance query returned invalid rows")
    latest: dict[tuple[str, str], tuple[str, datetime, int]] = {}
    for row in rows:
        if not isinstance(row, tuple) or len(row) != 5:
            raise ValueError("provenance query returned an invalid row")
        source, dataset, run_id, ingested_at, record_count = row
        if (source, dataset) not in _KNOWN_DATASETS:
            raise ValueError("provenance query returned an unknown dataset")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("provenance run ID is invalid")
        if not isinstance(ingested_at, datetime):
            raise ValueError("provenance recorded time is invalid")
        if isinstance(record_count, bool) or not isinstance(record_count, int) or record_count < 0:
            raise ValueError("provenance record count is invalid")
        latest[(source, dataset)] = (run_id, ingested_at, record_count)
    return latest


def _signal_measurement(signal: QualitySignal, name: str) -> date | int | None:
    for measurement in signal.measurements:
        if measurement.name == name:
            value = measurement.value
            if value is None or isinstance(value, (date, int)) and not isinstance(value, datetime):
                return value
            raise ValueError(f"recency measurement {name} is invalid")
    return None


def _format_human_report(report: OperationalStatusReport) -> str:
    lines = [f"FinStream operational status: {report.status.value.upper()} (as_of={report.as_of.isoformat()})"]
    for check in report.checks:
        lines.append(f"[{check.status.value.upper()}] {check.check_id}: {check.message}")
        for detail in check.details:
            lines.append(f"  {detail.name}: {_serialize_value(detail.value)}")
    return "\n".join(lines)


def _settings_secret_values(settings: Settings) -> tuple[str | None, ...]:
    return (
        settings.postgres_dsn,
        settings.twelve_data_api_key,
        settings.fred_api_key,
        settings.sec_user_agent,
    )


def _redact_text(value: str, secrets: tuple[str | None, ...]) -> str:
    return redact_operational_output(value, secrets=secrets)


def _serialize_value(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _validate_as_of(value: object) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError("as_of must be a date")


def _parse_as_of(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--as-of must be an ISO date") from exc


if __name__ == "__main__":
    raise SystemExit(main())
