"""Airflow-independent source-schema readiness runtime boundary."""

from finstream.config import Settings, load_settings
from finstream.database.connection import connect_postgres
from finstream.database.schema import SOURCE_SCHEMA_NAME, ensure_source_schema


SchemaReadinessSummary = dict[str, str]


def ensure_source_schema_ready(
    *,
    settings: Settings | None = None,
) -> SchemaReadinessSummary:
    """Ensure the source schema is ready within one connection-owned transaction."""
    active_settings = settings if settings is not None else load_settings()
    with connect_postgres(active_settings.postgres_dsn) as connection:
        ensure_source_schema(connection)
    return {"schema": SOURCE_SCHEMA_NAME, "status": "ready"}
