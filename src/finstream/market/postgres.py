"""Load validated Market Bronze Parquet runs into PostgreSQL source tables."""

from collections.abc import Iterable
from datetime import date

import psycopg
import pyarrow as pa

from finstream.bronze.json_storage import raw_json_path
from finstream.bronze.parquet_storage import parquet_path, read_parquet
from finstream.database.replay import (
    IngestionRun,
    IngestionRunReplayError,
    IngestionRunState,
    register_or_verify_ingestion_run,
)
from finstream.market.bronze import (
    DAILY_MARKET_PRICE_SCHEMA,
    MARKET_BRONZE_DATASET,
    MARKET_BRONZE_SOURCE,
    MarketBronzeResult,
)


_INSERT_INGESTION_RUN = """
    INSERT INTO source_data.ingestion_runs (
        source,
        dataset,
        run_id,
        ingested_at,
        raw_json_path,
        parquet_path,
        record_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source, dataset, run_id) DO NOTHING
    RETURNING 1
"""

_INSERT_MARKET_DAILY_PRICE = """
    INSERT INTO source_data.market_daily_prices (
        source,
        dataset,
        run_id,
        source_row_number,
        symbol,
        trading_date,
        open,
        high,
        low,
        close,
        volume
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_SELECT_LATEST_MARKET_TRADING_DATE = """
    SELECT max(trading_date)
    FROM source_data.market_daily_prices
    WHERE source = %s AND dataset = %s AND symbol = %s
"""


class MarketPostgresLoadError(ValueError):
    """Raised when a Market Bronze result cannot be loaded safely."""


def latest_market_trading_date(
    connection: psycopg.Connection,
    symbol: str,
) -> date | None:
    """Return the latest source-aligned Market date for one canonical symbol."""
    if not isinstance(symbol, str) or not symbol.strip():
        raise MarketPostgresLoadError("Market symbol must not be blank")
    normalized_symbol = symbol.strip().upper()

    with connection.cursor() as cursor:
        cursor.execute(
            _SELECT_LATEST_MARKET_TRADING_DATE,
            (MARKET_BRONZE_SOURCE, MARKET_BRONZE_DATASET, normalized_symbol),
        )
        row = cursor.fetchone()

    if row is None:
        raise MarketPostgresLoadError(
            "Market trading-date watermark query returned no row"
        )
    latest_trading_date = row[0]
    if latest_trading_date is not None and not isinstance(latest_trading_date, date):
        raise MarketPostgresLoadError(
            "Market trading-date watermark query returned an invalid date"
        )
    return latest_trading_date


def _validated_table(result: MarketBronzeResult) -> pa.Table:
    """Validate all local Bronze-to-PostgreSQL preconditions before SQL."""
    if not isinstance(result, MarketBronzeResult):
        raise MarketPostgresLoadError("result must be a MarketBronzeResult")

    metadata = result.location.metadata
    if metadata.source != MARKET_BRONZE_SOURCE:
        raise MarketPostgresLoadError("Market Bronze source must be twelve_data")
    if metadata.dataset != MARKET_BRONZE_DATASET:
        raise MarketPostgresLoadError(
            "Market Bronze dataset must be daily_market_prices"
        )
    if metadata.entity is not None and metadata.entity != result.symbol:
        raise MarketPostgresLoadError("Market Bronze entity does not match symbol")
    if result.raw_json_path != raw_json_path(result.location):
        raise MarketPostgresLoadError("Market Bronze raw JSON path is not canonical")
    if result.parquet_path != parquet_path(result.location):
        raise MarketPostgresLoadError("Market Bronze Parquet path is not canonical")
    if isinstance(result.record_count, bool) or not isinstance(result.record_count, int):
        raise MarketPostgresLoadError("Market Bronze record count must be an integer")
    if result.record_count < 0:
        raise MarketPostgresLoadError("Market Bronze record count must not be negative")

    table = read_parquet(result.location)
    if not table.schema.equals(DAILY_MARKET_PRICE_SCHEMA, check_metadata=True):
        raise MarketPostgresLoadError("Market Bronze Parquet schema does not match")
    try:
        table.validate(full=True)
    except pa.ArrowException as exc:
        raise MarketPostgresLoadError(
            "Market Bronze Parquet table failed Arrow validation"
        ) from exc
    if table.num_rows != result.record_count:
        raise MarketPostgresLoadError(
            "Market Bronze record count does not match Parquet row count"
        )
    if any(symbol != result.symbol for symbol in table.column("symbol").to_pylist()):
        raise MarketPostgresLoadError(
            "Market Bronze Parquet symbols do not match the result symbol"
        )
    return table


def _market_row_parameters(
    table: pa.Table,
    *,
    source: str,
    dataset: str,
    run_id: str,
) -> Iterable[tuple[object, ...]]:
    """Yield source-ordered parameter tuples without altering Arrow values."""
    for source_row_number, row in enumerate(table.to_pylist()):
        yield (
            source,
            dataset,
            run_id,
            source_row_number,
            row["symbol"],
            row["trading_date"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
        )


def _ingestion_run(result: MarketBronzeResult) -> IngestionRun:
    """Build replay-verification metadata after Market preflight succeeds."""
    metadata = result.location.metadata
    return IngestionRun(
        source=metadata.source,
        dataset=metadata.dataset,
        run_id=metadata.run_id,
        ingested_at=metadata.ingested_at,
        raw_json_path=str(result.raw_json_path),
        parquet_path=str(result.parquet_path),
        record_count=result.record_count,
        source_table="source_data.market_daily_prices",
    )


def load_market_bronze_to_postgres(
    connection: psycopg.Connection,
    result: MarketBronzeResult,
) -> int:
    """Load one existing Market Bronze run inside the caller-owned transaction."""
    table = _validated_table(result)
    metadata = result.location.metadata
    ingestion_run = _ingestion_run(result)

    with connection.cursor() as cursor:
        try:
            replay_state = register_or_verify_ingestion_run(
                cursor,
                run=ingestion_run,
                insert_sql=_INSERT_INGESTION_RUN,
            )
        except IngestionRunReplayError as exc:
            raise MarketPostgresLoadError(str(exc)) from exc
        if replay_state is IngestionRunState.VERIFIED_REPLAY:
            return result.record_count
        if result.record_count:
            cursor.executemany(
                _INSERT_MARKET_DAILY_PRICE,
                _market_row_parameters(
                    table,
                    source=metadata.source,
                    dataset=metadata.dataset,
                    run_id=metadata.run_id,
                ),
            )

    return result.record_count
