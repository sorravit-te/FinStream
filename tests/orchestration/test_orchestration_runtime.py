"""Unit tests for Airflow-independent source-runtime adapters."""

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from finstream.config import Settings
from finstream.orchestration import runtime


_RUN_AT = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
_SETTINGS = Settings(
    twelve_data_api_key="market-key",
    fred_api_key="fred-key",
    sec_user_agent="FinStream test@example.com",
    postgres_dsn="postgresql://test:fake@localhost:5432/finstream",
)


def _bronze_dataset(
    *,
    source: str,
    run_id: str,
    record_count: int,
) -> SimpleNamespace:
    return SimpleNamespace(
        location=SimpleNamespace(metadata=SimpleNamespace(source=source, run_id=run_id)),
        record_count=record_count,
    )


def _connection_context() -> tuple[MagicMock, MagicMock]:
    connection = MagicMock(name="connection")
    context = MagicMock(name="connection_context")
    context.__enter__.return_value = connection
    context.__exit__.return_value = False
    return context, connection


def test_market_runtime_uses_incremental_bronze_and_market_loader(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context, connection = _connection_context()
    client = MagicMock(name="twelve_data_client")
    service = MagicMock(name="market_service")
    bronze_result = _bronze_dataset(
        source="twelve_data", run_id="20260920T120000000000Z", record_count=3
    )
    bronze_result.symbol = "AAPL"
    service.ingest_symbol_incrementally_to_bronze.return_value = bronze_result
    client_factory = MagicMock(return_value=client)
    service_factory = MagicMock(return_value=service)
    loader = MagicMock(return_value=3)

    monkeypatch.setattr(runtime, "connect_postgres", MagicMock(return_value=context))
    monkeypatch.setattr(runtime, "TwelveDataClient", client_factory)
    monkeypatch.setattr(runtime, "MarketIngestionService", service_factory)
    monkeypatch.setattr(runtime, "load_market_bronze_to_postgres", loader)

    result = runtime.run_market_source(
        " aapl ", run_at=_RUN_AT, settings=_SETTINGS, bronze_root=tmp_path
    )

    client_factory.assert_called_once_with("market-key")
    service_factory.assert_called_once_with(client)
    service.ingest_symbol_incrementally_to_bronze.assert_called_once_with(
        connection, " aapl ", run_at=_RUN_AT, bronze_root=tmp_path
    )
    assert service.ingest_symbol_incrementally_to_bronze.call_args.kwargs["run_at"] is _RUN_AT
    loader.assert_called_once_with(connection, bronze_result)
    context.__exit__.assert_called_once_with(None, None, None)
    assert result == {
        "source": "twelve_data",
        "symbol": "AAPL",
        "run_id": "20260920T120000000000Z",
        "record_count": 3,
        "records_loaded": 3,
    }


def test_sec_runtime_uses_complete_company_bronze_and_combined_loader(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context, connection = _connection_context()
    client = MagicMock(name="sec_edgar_client")
    service = MagicMock(name="sec_service")
    submissions = _bronze_dataset(
        source="sec_edgar", run_id="20260920T120000000000Z", record_count=2
    )
    company_facts = _bronze_dataset(
        source="sec_edgar", run_id="20260920T120000000000Z", record_count=5
    )
    bronze_result = SimpleNamespace(
        cik="0000320193", submissions=submissions, company_facts=company_facts
    )
    service.ingest_company_to_bronze.return_value = bronze_result
    client_factory = MagicMock(return_value=client)
    service_factory = MagicMock(return_value=service)
    loader = MagicMock(
        return_value=SimpleNamespace(submissions_loaded=2, company_facts_loaded=5)
    )

    monkeypatch.setattr(runtime, "connect_postgres", MagicMock(return_value=context))
    monkeypatch.setattr(runtime, "SecEdgarClient", client_factory)
    monkeypatch.setattr(runtime, "SecFinancialIngestionService", service_factory)
    monkeypatch.setattr(runtime, "load_sec_company_bronze_to_postgres", loader)

    result = runtime.run_sec_source(
        320193, run_at=_RUN_AT, settings=_SETTINGS, bronze_root=tmp_path
    )

    client_factory.assert_called_once_with("FinStream test@example.com")
    service_factory.assert_called_once_with(client)
    service.ingest_company_to_bronze.assert_called_once_with(
        320193, run_at=_RUN_AT, bronze_root=tmp_path
    )
    assert service.ingest_company_to_bronze.call_args.kwargs["run_at"] is _RUN_AT
    loader.assert_called_once_with(connection, bronze_result)
    context.__exit__.assert_called_once_with(None, None, None)
    assert result == {
        "source": "sec_edgar",
        "cik": "0000320193",
        "run_id": "20260920T120000000000Z",
        "submissions_record_count": 2,
        "company_facts_record_count": 5,
        "submissions_loaded": 2,
        "company_facts_loaded": 5,
    }


def test_fred_runtime_uses_incremental_bronze_and_combined_loader(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context, connection = _connection_context()
    client = MagicMock(name="fred_client")
    service = MagicMock(name="fred_service")
    metadata = _bronze_dataset(
        source="fred", run_id="20260920T120000000000Z", record_count=1
    )
    observations = _bronze_dataset(
        source="fred", run_id="20260920T120000000000Z", record_count=4
    )
    bronze_result = SimpleNamespace(
        series_id="DFF", metadata=metadata, observations=observations
    )
    service.ingest_series_incrementally_to_bronze.return_value = bronze_result
    client_factory = MagicMock(return_value=client)
    service_factory = MagicMock(return_value=service)
    loader = MagicMock(
        return_value=SimpleNamespace(metadata_loaded=1, observations_loaded=4)
    )

    monkeypatch.setattr(runtime, "connect_postgres", MagicMock(return_value=context))
    monkeypatch.setattr(runtime, "FredClient", client_factory)
    monkeypatch.setattr(runtime, "FredMacroeconomicIngestionService", service_factory)
    monkeypatch.setattr(runtime, "load_fred_series_bronze_to_postgres", loader)

    result = runtime.run_fred_source(
        " DFF ", run_at=_RUN_AT, settings=_SETTINGS, bronze_root=tmp_path
    )

    client_factory.assert_called_once_with("fred-key")
    service_factory.assert_called_once_with(client)
    service.ingest_series_incrementally_to_bronze.assert_called_once_with(
        connection, " DFF ", run_at=_RUN_AT, bronze_root=tmp_path
    )
    assert service.ingest_series_incrementally_to_bronze.call_args.kwargs["run_at"] is _RUN_AT
    loader.assert_called_once_with(connection, bronze_result)
    context.__exit__.assert_called_once_with(None, None, None)
    assert result == {
        "source": "fred",
        "series_id": "DFF",
        "run_id": "20260920T120000000000Z",
        "metadata_record_count": 1,
        "observations_record_count": 4,
        "metadata_loaded": 1,
        "observations_loaded": 4,
    }


def test_loader_failure_exits_connection_context_without_successful_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, connection = _connection_context()
    service = MagicMock(name="market_service")
    bronze_result = _bronze_dataset(
        source="twelve_data", run_id="20260920T120000000000Z", record_count=3
    )
    bronze_result.symbol = "AAPL"
    service.ingest_symbol_incrementally_to_bronze.return_value = bronze_result
    failure = RuntimeError("market loader failed")

    monkeypatch.setattr(runtime, "connect_postgres", MagicMock(return_value=context))
    monkeypatch.setattr(runtime, "TwelveDataClient", MagicMock())
    monkeypatch.setattr(runtime, "MarketIngestionService", MagicMock(return_value=service))
    monkeypatch.setattr(
        runtime, "load_market_bronze_to_postgres", MagicMock(side_effect=failure)
    )

    with pytest.raises(RuntimeError, match="market loader failed"):
        runtime.run_market_source("AAPL", run_at=_RUN_AT, settings=_SETTINGS)

    context.__exit__.assert_called_once()
    exception_type, exception, traceback = context.__exit__.call_args.args
    assert exception_type is RuntimeError
    assert exception is failure
    assert traceback is not None
    assert connection is service.ingest_symbol_incrementally_to_bronze.call_args.args[0]


def test_runtime_package_does_not_import_airflow() -> None:
    package_directory = Path(runtime.__file__).parent

    for module_path in package_directory.glob("*.py"):
        source = module_path.read_text(encoding="utf-8").lower()
        assert "import airflow" not in source
        assert "from airflow" not in source
