"""Market-specific Bronze schema, conversion, and result contracts."""

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from finstream.bronze.models import BronzeRunLocation
from finstream.market.models import DailyMarketPrice


MARKET_BRONZE_SOURCE = "twelve_data"
MARKET_BRONZE_DATASET = "daily_market_prices"

DAILY_MARKET_PRICE_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("open", pa.decimal128(38, 18), nullable=False),
        pa.field("high", pa.decimal128(38, 18), nullable=False),
        pa.field("low", pa.decimal128(38, 18), nullable=False),
        pa.field("close", pa.decimal128(38, 18), nullable=False),
        pa.field("volume", pa.int64(), nullable=True),
    ]
)


class MarketBronzeValidationError(ValueError):
    """Raised when market records cannot form the Bronze Arrow table."""


@dataclass(frozen=True)
class MarketBronzeResult:
    """Locations and record count for one persisted Market Bronze run."""

    symbol: str
    location: BronzeRunLocation
    raw_json_path: Path
    parquet_path: Path
    record_count: int


def daily_market_prices_to_table(
    records: Iterable[DailyMarketPrice],
) -> pa.Table:
    """Convert validated daily market records to the stable Bronze schema."""
    rows: list[dict[str, object]] = []
    for index, record in enumerate(records):
        if not isinstance(record, DailyMarketPrice):
            raise MarketBronzeValidationError(
                f"Market record {index} is not a DailyMarketPrice"
            )
        rows.append(
            {
                "symbol": record.symbol,
                "trading_date": record.trading_date,
                "open": record.open,
                "high": record.high,
                "low": record.low,
                "close": record.close,
                "volume": record.volume,
            }
        )

    try:
        table = pa.Table.from_pylist(rows, schema=DAILY_MARKET_PRICE_SCHEMA)
        table.validate(full=True)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise MarketBronzeValidationError(
            "Market records cannot be represented by the Bronze Arrow schema"
        ) from exc
    return table

