"""Source-aligned FRED series models."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


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


@dataclass(frozen=True)
class FredObservation:
    """One source-aligned observation from a FRED response."""

    series_id: str
    realtime_start: date
    realtime_end: date
    observation_date: date
    value: Decimal | None


@dataclass(frozen=True)
class FredSeriesObservations:
    """Parsed observations for one requested FRED series."""

    series_id: str
    observations: tuple[FredObservation, ...]


@dataclass(frozen=True)
class FredSeriesSourceData:
    """Successfully parsed source data for one FRED series."""

    series_id: str
    metadata: FredSeriesMetadata
    observations: FredSeriesObservations
