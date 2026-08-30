from types import ModuleType
import traceback
from unittest.mock import Mock

import psycopg
import pytest

import finstream.database.connection as database_connection
from finstream.database.connection import (
    PostgresConfigurationError,
    connect_postgres,
    validate_postgres_dsn,
)


_POSTGRES_URL = "postgresql://finstream:fake-password@localhost:5432/finstream"
_POSTGRES_CONNINFO = (
    "host=localhost port=5432 dbname=finstream "
    "user=finstream password=fake-password"
)


@pytest.mark.parametrize("dsn", [_POSTGRES_URL, _POSTGRES_CONNINFO])
def test_validate_postgres_dsn_accepts_psycopg_conninfo(dsn: str) -> None:
    assert validate_postgres_dsn(dsn) == dsn


def test_validate_postgres_dsn_strips_outer_whitespace() -> None:
    assert validate_postgres_dsn(f"  {_POSTGRES_URL}  ") == _POSTGRES_URL


@pytest.mark.parametrize(
    "dsn",
    [
        pytest.param(None, id="missing"),
        pytest.param(" \t ", id="blank"),
        pytest.param(123, id="wrong-runtime-type"),
    ],
)
def test_validate_postgres_dsn_rejects_missing_blank_and_wrong_type(
    dsn: object,
) -> None:
    with pytest.raises(PostgresConfigurationError):
        validate_postgres_dsn(dsn)  # type: ignore[arg-type]


def test_invalid_conninfo_is_safe_and_never_attempts_a_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    malformed_dsn = "host='unterminated password=obvious-fake-password"
    connect = Mock()
    monkeypatch.setattr(database_connection.psycopg, "connect", connect)

    with pytest.raises(PostgresConfigurationError) as error:
        connect_postgres(malformed_dsn)

    assert malformed_dsn not in str(error.value)
    assert "obvious-fake-password" not in str(error.value)
    connect.assert_not_called()
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__ is True
    rendered_traceback = "".join(traceback.format_exception(error.value))
    assert malformed_dsn not in rendered_traceback
    assert "obvious-fake-password" not in rendered_traceback


def test_connect_postgres_passes_clean_dsn_once_with_transactions_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = object()
    connect = Mock(return_value=connection)
    monkeypatch.setattr(database_connection.psycopg, "connect", connect)

    assert connect_postgres(f"  {_POSTGRES_URL}  ") is connection
    connect.assert_called_once_with(_POSTGRES_URL, autocommit=False)


def test_runtime_psycopg_error_propagates_without_reclassification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = Mock(side_effect=psycopg.OperationalError("database unavailable"))
    monkeypatch.setattr(database_connection.psycopg, "connect", connect)

    with pytest.raises(psycopg.OperationalError, match="database unavailable"):
        connect_postgres(_POSTGRES_URL)

    connect.assert_called_once_with(_POSTGRES_URL, autocommit=False)


def test_database_package_does_not_depend_on_provider_packages() -> None:
    provider_prefixes = (
        "finstream.market",
        "finstream.sec",
        "finstream.fred",
    )

    for value in vars(database_connection).values():
        referenced_module = (
            value.__name__
            if isinstance(value, ModuleType)
            else getattr(value, "__module__", "")
        )
        assert not referenced_module.startswith(provider_prefixes)
