"""Unit tests for the Airflow-independent source-schema runtime boundary."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from finstream.config import Settings
from finstream.orchestration import schema_runtime


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


def test_schema_readiness_uses_existing_configuration_connection_and_schema_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, connection = _connection_context()
    connect = MagicMock(return_value=context)
    ensure_schema = MagicMock()
    monkeypatch.setattr(schema_runtime, "connect_postgres", connect)
    monkeypatch.setattr(schema_runtime, "ensure_source_schema", ensure_schema)

    result = schema_runtime.ensure_source_schema_ready(settings=_SETTINGS)

    connect.assert_called_once_with(_SETTINGS.postgres_dsn)
    ensure_schema.assert_called_once_with(connection)
    context.__exit__.assert_called_once_with(None, None, None)
    assert result == {"schema": "source_data", "status": "ready"}
    assert _SETTINGS.postgres_dsn not in result.values()


def test_schema_readiness_propagates_failure_to_connection_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, _ = _connection_context()
    failure = RuntimeError("schema failed")
    monkeypatch.setattr(schema_runtime, "connect_postgres", MagicMock(return_value=context))
    monkeypatch.setattr(
        schema_runtime,
        "ensure_source_schema",
        MagicMock(side_effect=failure),
    )

    with pytest.raises(RuntimeError, match="schema failed"):
        schema_runtime.ensure_source_schema_ready(settings=_SETTINGS)

    exception_type, exception, traceback = context.__exit__.call_args.args
    assert exception_type is RuntimeError
    assert exception is failure
    assert traceback is not None


def test_schema_readiness_uses_default_settings_when_not_supplied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, connection = _connection_context()
    load_settings = MagicMock(return_value=_SETTINGS)
    monkeypatch.setattr(schema_runtime, "load_settings", load_settings)
    monkeypatch.setattr(schema_runtime, "connect_postgres", MagicMock(return_value=context))
    ensure_schema = MagicMock()
    monkeypatch.setattr(schema_runtime, "ensure_source_schema", ensure_schema)

    schema_runtime.ensure_source_schema_ready()

    load_settings.assert_called_once_with()
    ensure_schema.assert_called_once_with(connection)


def test_schema_runtime_module_does_not_import_airflow() -> None:
    source = Path(schema_runtime.__file__).read_text(encoding="utf-8").lower()

    assert "import airflow" not in source
    assert "from airflow" not in source
