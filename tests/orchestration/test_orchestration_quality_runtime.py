"""Unit tests for the Airflow-independent quality monitoring runtime."""

import json
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from finstream.config import Settings
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
)
from finstream.fred.series import INITIAL_FRED_SERIES_IDS
from finstream.market.bronze import MARKET_BRONZE_DATASET, MARKET_BRONZE_SOURCE
from finstream.orchestration import quality_runtime
from finstream.quality.monitoring import QualityMeasurement, QualitySignal, QualityStatus
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_SUBMISSIONS_BRONZE_DATASET,
)
from finstream.sec.companies import INITIAL_SEC_COMPANIES


_AS_OF = date(2026, 9, 21)
_SETTINGS = Settings(
    twelve_data_api_key="market-key",
    fred_api_key="fred-key",
    sec_user_agent="FinStream test@example.com",
    postgres_dsn="postgresql://test:fake@localhost:5432/finstream",
)


def _connection_context() -> tuple[MagicMock, MagicMock]:
    connection = MagicMock(name="connection")
    context = MagicMock(name="connection_context")
    context.__enter__.return_value = connection
    context.__exit__.return_value = False
    return context, connection


def _signal(
    *,
    status: QualityStatus = QualityStatus.INFO,
    source: str = "source",
    dataset: str = "dataset",
    entity_id: str | None = None,
) -> QualitySignal:
    return QualitySignal(
        check_id="check",
        source=source,
        dataset=dataset,
        entity_id=entity_id,
        dimension="freshness",
        status=status,
        as_of=_AS_OF,
        measurements=(
            QualityMeasurement("observed_date", date(2026, 9, 20)),
            QualityMeasurement(
                "observed_at", datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
            ),
        ),
        message="Informational measurement.",
    )


def test_quality_runtime_executes_all_approved_checks_and_serializes_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, connection = _connection_context()
    market = MagicMock(side_effect=lambda *_args, **_kwargs: _signal())
    fred = MagicMock(side_effect=lambda *_args, **_kwargs: _signal())
    sec_submissions = MagicMock(side_effect=lambda *_args, **_kwargs: _signal())
    sec_facts = MagicMock(side_effect=lambda *_args, **_kwargs: _signal())
    run_count = MagicMock(side_effect=lambda *_args, **_kwargs: _signal())
    connect = MagicMock(return_value=context)
    monkeypatch.setattr(quality_runtime, "connect_postgres", connect)
    monkeypatch.setattr(quality_runtime, "monitor_market_freshness", market)
    monkeypatch.setattr(quality_runtime, "monitor_fred_observation_freshness", fred)
    monkeypatch.setattr(quality_runtime, "monitor_sec_submissions_freshness", sec_submissions)
    monkeypatch.setattr(quality_runtime, "monitor_sec_company_facts_freshness", sec_facts)
    monkeypatch.setattr(quality_runtime, "monitor_run_count_movement", run_count)

    result = quality_runtime.run_quality_monitoring(as_of=_AS_OF, settings=_SETTINGS)

    assert [call.args[1] for call in market.call_args_list] == [
        company.ticker for company in INITIAL_SEC_COMPANIES
    ]
    assert [call.args[1] for call in fred.call_args_list] == list(INITIAL_FRED_SERIES_IDS)
    assert [call.args[1] for call in sec_submissions.call_args_list] == [
        company.cik for company in INITIAL_SEC_COMPANIES
    ]
    assert [call.args[1] for call in sec_facts.call_args_list] == [
        company.cik for company in INITIAL_SEC_COMPANIES
    ]
    assert [(call.kwargs["source"], call.kwargs["dataset"]) for call in run_count.call_args_list] == [
        (MARKET_BRONZE_SOURCE, MARKET_BRONZE_DATASET),
        (SEC_BRONZE_SOURCE, SEC_SUBMISSIONS_BRONZE_DATASET),
        (SEC_BRONZE_SOURCE, SEC_COMPANY_FACTS_BRONZE_DATASET),
        (FRED_BRONZE_SOURCE, FRED_SERIES_METADATA_BRONZE_DATASET),
        (FRED_BRONZE_SOURCE, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET),
    ]
    for monitor in (market, fred, sec_submissions, sec_facts, run_count):
        assert all(call.kwargs["as_of"] is _AS_OF for call in monitor.call_args_list)
        assert all(call.args[0] is connection for call in monitor.call_args_list)
    connect.assert_called_once_with(_SETTINGS.postgres_dsn)
    context.__exit__.assert_called_once_with(None, None, None)
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    connection.close.assert_not_called()
    assert result["as_of"] == "2026-09-21"
    assert result["signal_count"] == len(INITIAL_SEC_COMPANIES) + len(
        INITIAL_FRED_SERIES_IDS
    ) + 2 * len(INITIAL_SEC_COMPANIES) + 5
    assert result["status_counts"] == {"pass": 0, "warning": 0, "info": 28, "error": 0}
    assert result["signals"][0]["measurements"] == [
        {"name": "observed_date", "value": "2026-09-20"},
        {"name": "observed_at", "value": "2026-09-20T12:00:00+00:00"},
    ]
    assert _SETTINGS.postgres_dsn not in json.dumps(result)
    json.dumps(result)


def test_quality_runtime_keeps_returned_error_signals_non_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, _ = _connection_context()
    error = _signal(status=QualityStatus.ERROR)
    monkeypatch.setattr(quality_runtime, "connect_postgres", MagicMock(return_value=context))
    monkeypatch.setattr(quality_runtime, "monitor_market_freshness", MagicMock(return_value=error))
    monkeypatch.setattr(quality_runtime, "monitor_fred_observation_freshness", MagicMock(return_value=error))
    monkeypatch.setattr(quality_runtime, "monitor_sec_submissions_freshness", MagicMock(return_value=error))
    monkeypatch.setattr(quality_runtime, "monitor_sec_company_facts_freshness", MagicMock(return_value=error))
    monkeypatch.setattr(quality_runtime, "monitor_run_count_movement", MagicMock(return_value=error))

    result = quality_runtime.run_quality_monitoring(as_of=_AS_OF, settings=_SETTINGS)

    assert result["status_counts"]["error"] == result["signal_count"]
    assert context.__exit__.call_args.args == (None, None, None)


def test_quality_runtime_propagates_unexpected_runtime_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, _ = _connection_context()
    failure = RuntimeError("unexpected monitoring failure")
    monkeypatch.setattr(quality_runtime, "connect_postgres", MagicMock(return_value=context))
    monkeypatch.setattr(
        quality_runtime,
        "monitor_market_freshness",
        MagicMock(side_effect=failure),
    )

    with pytest.raises(RuntimeError, match="unexpected monitoring failure"):
        quality_runtime.run_quality_monitoring(as_of=_AS_OF, settings=_SETTINGS)

    assert context.__exit__.call_args.args[0] is RuntimeError


def test_quality_runtime_requires_an_explicit_date_and_has_no_airflow_dependency() -> None:
    with pytest.raises(ValueError, match="as_of"):
        quality_runtime.run_quality_monitoring(
            as_of=datetime(2026, 9, 21, tzinfo=timezone.utc), settings=_SETTINGS
        )

    source = quality_runtime.__file__
    assert source is not None
    text = Path(source).read_text(encoding="utf-8").lower()
    assert "import airflow" not in text
    assert "from airflow" not in text
