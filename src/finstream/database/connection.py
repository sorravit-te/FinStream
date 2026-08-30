"""Synchronous PostgreSQL connection configuration boundary."""

import psycopg
from psycopg import conninfo


class PostgresConfigurationError(ValueError):
    """Raised when local PostgreSQL connection configuration is invalid."""


def validate_postgres_dsn(dsn: str | None) -> str:
    """Validate and normalize PostgreSQL conninfo without connecting."""
    if not isinstance(dsn, str):
        raise PostgresConfigurationError("PostgreSQL DSN must be a string")

    cleaned_dsn = dsn.strip()
    if not cleaned_dsn:
        raise PostgresConfigurationError("PostgreSQL DSN must not be blank")

    try:
        conninfo.conninfo_to_dict(cleaned_dsn)
    except psycopg.ProgrammingError:
        raise PostgresConfigurationError("PostgreSQL DSN is invalid") from None

    return cleaned_dsn


def connect_postgres(dsn: str | None) -> psycopg.Connection:
    """Create one transactional Psycopg connection from a validated DSN."""
    return psycopg.connect(validate_postgres_dsn(dsn), autocommit=False)
