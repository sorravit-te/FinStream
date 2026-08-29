"""Source-aligned market data records."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class DailyMarketPrice:
    """One daily OHLCV observation for a market symbol."""

    symbol: str
    trading_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int | None
