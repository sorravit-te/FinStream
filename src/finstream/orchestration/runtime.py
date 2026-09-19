"""Thin source-runtime adapters for future orchestration tasks."""

from datetime import datetime
from pathlib import Path

from finstream.bronze.paths import DEFAULT_BRONZE_ROOT
from finstream.config import Settings, load_settings
from finstream.database.connection import connect_postgres
from finstream.fred.ingestion import FredMacroeconomicIngestionService
from finstream.fred.postgres import load_fred_series_bronze_to_postgres
from finstream.fred.client import FredClient
from finstream.market.ingestion import MarketIngestionService
from finstream.market.postgres import load_market_bronze_to_postgres
from finstream.market.twelve_data import TwelveDataClient
from finstream.sec.edgar import SecEdgarClient
from finstream.sec.ingestion import SecFinancialIngestionService
from finstream.sec.postgres import load_sec_company_bronze_to_postgres


RuntimeSummary = dict[str, str | int]


def run_market_source(
    symbol: str,
    *,
    run_at: datetime,
    settings: Settings | None = None,
    bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
) -> RuntimeSummary:
    """Ingest and load one Market symbol through existing incremental boundaries."""
    active_settings = _active_settings(settings)
    service = MarketIngestionService(TwelveDataClient(active_settings.twelve_data_api_key))

    with connect_postgres(active_settings.postgres_dsn) as connection:
        bronze_result = service.ingest_symbol_incrementally_to_bronze(
            connection,
            symbol,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        records_loaded = load_market_bronze_to_postgres(connection, bronze_result)
        metadata = bronze_result.location.metadata
        summary: RuntimeSummary = {
            "source": metadata.source,
            "symbol": bronze_result.symbol,
            "run_id": metadata.run_id,
            "record_count": bronze_result.record_count,
            "records_loaded": records_loaded,
        }

    return summary


def run_sec_source(
    cik: str | int,
    *,
    run_at: datetime,
    settings: Settings | None = None,
    bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
) -> RuntimeSummary:
    """Ingest and load one complete SEC company source run."""
    active_settings = _active_settings(settings)
    service = SecFinancialIngestionService(
        SecEdgarClient(active_settings.sec_user_agent)
    )

    with connect_postgres(active_settings.postgres_dsn) as connection:
        bronze_result = service.ingest_company_to_bronze(
            cik,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        load_result = load_sec_company_bronze_to_postgres(connection, bronze_result)
        metadata = bronze_result.submissions.location.metadata
        summary: RuntimeSummary = {
            "source": metadata.source,
            "cik": bronze_result.cik,
            "run_id": metadata.run_id,
            "submissions_record_count": bronze_result.submissions.record_count,
            "company_facts_record_count": bronze_result.company_facts.record_count,
            "submissions_loaded": load_result.submissions_loaded,
            "company_facts_loaded": load_result.company_facts_loaded,
        }

    return summary


def run_fred_source(
    series_id: str,
    *,
    run_at: datetime,
    settings: Settings | None = None,
    bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
) -> RuntimeSummary:
    """Ingest and load one FRED series through existing incremental boundaries."""
    active_settings = _active_settings(settings)
    service = FredMacroeconomicIngestionService(FredClient(active_settings.fred_api_key))

    with connect_postgres(active_settings.postgres_dsn) as connection:
        bronze_result = service.ingest_series_incrementally_to_bronze(
            connection,
            series_id,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        load_result = load_fred_series_bronze_to_postgres(connection, bronze_result)
        metadata = bronze_result.metadata.location.metadata
        summary: RuntimeSummary = {
            "source": metadata.source,
            "series_id": bronze_result.series_id,
            "run_id": metadata.run_id,
            "metadata_record_count": bronze_result.metadata.record_count,
            "observations_record_count": bronze_result.observations.record_count,
            "metadata_loaded": load_result.metadata_loaded,
            "observations_loaded": load_result.observations_loaded,
        }

    return summary


def _active_settings(settings: Settings | None) -> Settings:
    """Use supplied settings or retain the established environment configuration path."""
    return load_settings() if settings is None else settings
