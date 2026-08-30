"""Source-aligned FRED series metadata models."""

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class FredSeriesMetadata:
    """Metadata for one series from a FRED series response."""

    series_id: str
    realtime_start: date
    realtime_end: date
    title: str
    observation_start: date
    observation_end: date
    frequency: str
    frequency_short: str
    units: str
    units_short: str
    seasonal_adjustment: str
    seasonal_adjustment_short: str
    last_updated: datetime
    popularity: int
    notes: str | None
