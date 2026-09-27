from datetime import date, datetime, timezone
import io
import json
from pathlib import Path
from unittest.mock import MagicMock

import psycopg
import pytest

from finstream.config import Settings
from finstream.operational_status import (
    OperationalCheck,
    OperationalDetail,
    OperationalStatus,
    OperationalStatusReport,
    _analytics_check,
    _bronze_check,
    _provenance_check,
    collect_operational_status,
    main,
    redact_operational_output,
    serialize_operational_status,
)
from finstream.quality.monitoring import QualityMeasurement, QualitySignal, QualityStatus
import finstream.operational_status as operational_status


_AS_OF = date(2026, 9, 27)
_SECRET_DSN = "postgresql://finstream:super-secret@localhost:5432/finstream_test"
_SETTINGS = Settings(
    twelve_data_api_key="twelve-secret",
    fred_api_key="fred-secret",
    sec_user_agent="FinStream test@example.com",
    postgres_dsn=_SECRET_DSN,
)


def _connection_context() -> tuple[MagicMock, MagicMock, MagicMock]:
    connection = MagicMock(name="connection")
    context = MagicMock(name="connection_context")
    context.__enter__.return_value = connection
    context.__exit__.return_value = False
    cursor = connection.cursor.return_value.__enter__.return_value
    return context, connection, cursor


def _freshness_signal() -> QualitySignal:
    return QualitySignal(
        check_id="market_latest_trading_date",
        source="twelve_data",
        dataset="daily_market_prices",
        entity_id="AAPL",
        dimension="freshness",
        status=QualityStatus.INFO,
        as_of=_AS_OF,
        measurements=(
            QualityMeasurement("latest_date", date(2026, 9, 26)),
            QualityMeasurement("age_calendar_days", 1),
        ),
        message="Informational measurement.",
    )


def test_collect_status_reports_read_only_database_provenance_analytics_and_recency(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context, connection, cursor = _connection_context()
    cursor.fetchone.side_effect = [
        ("finstream_test", "finstream"),
        ("analytics.mart_company_daily_performance",),
        ("analytics.mart_company_financial_growth",),
        ("analytics.mart_market_macro",),
    ]
    cursor.fetchall.return_value = [
        (
            "twelve_data",
            "daily_market_prices",
            "20260927T120000000000Z",
            datetime(2026, 9, 27, 12, tzinfo=timezone.utc),
            2,
        )
    ]
    monkeypatch.setattr(operational_status, "connect_postgres", MagicMock(return_value=context))
    monkeypatch.setattr(operational_status, "collect_quality_signals", MagicMock(return_value=[_freshness_signal()]))

    report = collect_operational_status(
        as_of=_AS_OF,
        settings=_SETTINGS,
        bronze_root=tmp_path / "bronze",
    )

    assert report.status is OperationalStatus.INFO
    checks = {check.check_id: check for check in report.checks}
    assert checks["postgresql"].status is OperationalStatus.OK
    assert checks["provenance"].status is OperationalStatus.INFO
    assert checks["analytics_relations"].status is OperationalStatus.OK
    assert checks["recency"].status is OperationalStatus.INFO
    assert "threshold" in checks["recency"].message
    assert checks["bronze"].status is OperationalStatus.INFO
    assert _SECRET_DSN not in json.dumps(serialize_operational_status(report))
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()


def test_unavailable_database_returns_redacted_error_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        operational_status,
        "connect_postgres",
        MagicMock(side_effect=psycopg.OperationalError(f"failed for {_SECRET_DSN}")),
    )

    report = collect_operational_status(
        as_of=_AS_OF,
        settings=_SETTINGS,
        bronze_root=tmp_path / "bronze",
    )

    assert report.status is OperationalStatus.ERROR
    postgres = report.checks[0]
    assert postgres.check_id == "postgresql"
    assert postgres.status is OperationalStatus.ERROR
    assert "super-secret" not in postgres.message
    assert _SECRET_DSN not in postgres.message


def test_empty_provenance_is_informational() -> None:
    _, connection, cursor = _connection_context()
    cursor.fetchall.return_value = []

    check = _provenance_check(connection)

    assert check.status is OperationalStatus.INFO
    assert "empty datasets are valid" in check.message
    assert any(detail.value is None for detail in check.details)
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()


def test_missing_expected_analytics_relations_are_operational_errors() -> None:
    _, connection, cursor = _connection_context()
    cursor.fetchone.side_effect = [(None,), (None,), (None,)]

    check = _analytics_check(connection)

    assert check.status is OperationalStatus.ERROR
    assert "mart_company_daily_performance" in check.message


def test_bronze_check_reports_only_the_latest_partition_without_mutation(
    tmp_path: Path,
) -> None:
    latest = (
        tmp_path
        / "twelve_data"
        / "daily_market_prices"
        / "ingestion_date=2026-09-27"
        / "run_id=20260927T120000000000Z"
    )
    latest.mkdir(parents=True)
    (latest / "payload.json").write_text("{}\n", encoding="utf-8")
    (latest / "data.parquet").write_bytes(b"not-read-by-status")

    check = _bronze_check(tmp_path)

    assert check.status is OperationalStatus.INFO
    details = {detail.name: detail.value for detail in check.details}
    assert details["twelve_data.daily_market_prices.latest_artifact_pair_present"] is True


def test_redaction_removes_url_conninfo_and_explicit_secrets() -> None:
    rendered = redact_operational_output(
        "postgresql://alice:url-secret@db/finstream password=conninfo-secret twelve-secret",
        secrets=("twelve-secret",),
    )

    assert "url-secret" not in rendered
    assert "conninfo-secret" not in rendered
    assert "twelve-secret" not in rendered
    assert "***" in rendered


def test_json_cli_output_is_stable_and_exit_code_is_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = OperationalStatusReport(
        as_of=_AS_OF,
        checks=(
            OperationalCheck(
                check_id="postgresql",
                status=OperationalStatus.OK,
                message="PostgreSQL is reachable.",
                details=(OperationalDetail("database", "finstream_test"),),
            ),
            OperationalCheck(
                check_id="recency",
                status=OperationalStatus.INFO,
                message="Informational.",
                details=(),
            ),
        ),
    )
    monkeypatch.setattr(operational_status, "load_settings", lambda: _SETTINGS)
    monkeypatch.setattr(operational_status, "collect_operational_status", lambda **_kwargs: report)
    output = io.StringIO()

    exit_code = main(["--as-of", _AS_OF.isoformat(), "--json"], stdout=output)

    assert exit_code == 0
    assert json.loads(output.getvalue()) == serialize_operational_status(report)


def test_cli_returns_nonzero_for_operational_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = OperationalStatusReport(
        as_of=_AS_OF,
        checks=(
            OperationalCheck(
                check_id="postgresql",
                status=OperationalStatus.ERROR,
                message="unavailable",
                details=(),
            ),
        ),
    )
    monkeypatch.setattr(operational_status, "load_settings", lambda: _SETTINGS)
    monkeypatch.setattr(operational_status, "collect_operational_status", lambda **_kwargs: report)

    assert main(["--as-of", _AS_OF.isoformat()], stdout=io.StringIO()) == 1
