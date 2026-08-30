"""Explicit live verification of the configured FRED ingestion stack."""

from datetime import date, timedelta

from finstream.config import load_settings
from finstream.fred.client import FredClient
from finstream.fred.ingestion import FredMacroeconomicIngestionService
from finstream.fred.models import FredSeriesSourceData
from finstream.fred.series import INITIAL_FRED_SERIES_IDS


def _verify_series(
    configured_id: str,
    source_data: FredSeriesSourceData,
) -> None:
    metadata = source_data.metadata
    observations = source_data.observations

    if not (
        source_data.series_id
        == metadata.series_id
        == observations.series_id
        == configured_id
    ):
        raise RuntimeError(f"FRED identity verification failed for {configured_id}")
    if not metadata.title.strip():
        raise RuntimeError(f"FRED title is blank for {configured_id}")
    if not metadata.frequency.strip() or not metadata.frequency_short.strip():
        raise RuntimeError(f"FRED frequency is blank for {configured_id}")
    if not metadata.units.strip():
        raise RuntimeError(f"FRED units are blank for {configured_id}")
    if metadata.observation_start > metadata.observation_end:
        raise RuntimeError(f"FRED observation range is invalid for {configured_id}")
    if metadata.realtime_start > metadata.realtime_end:
        raise RuntimeError(f"FRED real-time range is invalid for {configured_id}")
    if (
        metadata.last_updated.tzinfo is None
        or metadata.last_updated.utcoffset() is None
    ):
        raise RuntimeError(f"FRED last_updated lacks timezone for {configured_id}")
    if not observations.observations:
        raise RuntimeError(f"FRED returned no observations for {configured_id}")

    for observation in observations.observations:
        if observation.series_id != configured_id:
            raise RuntimeError(
                f"FRED observation identity mismatch for {configured_id}"
            )
        if not isinstance(observation.observation_date, date):
            raise RuntimeError(f"FRED observation date is invalid for {configured_id}")
        if observation.realtime_start > observation.realtime_end:
            raise RuntimeError(
                f"FRED observation real-time range is invalid for {configured_id}"
            )

    numeric_count = sum(
        observation.value is not None
        for observation in observations.observations
    )
    missing_count = len(observations.observations) - numeric_count
    if numeric_count == 0:
        raise RuntimeError(f"FRED returned no numeric values for {configured_id}")

    print(
        f"{configured_id}: title={metadata.title!r}; "
        f"frequency={metadata.frequency!r}; units={metadata.units!r}; "
        f"observations={len(observations.observations)}; "
        f"numeric={numeric_count}; missing={missing_count}; "
        f"first_date={observations.observations[0].observation_date}; "
        f"last_date={observations.observations[-1].observation_date}; "
        f"last_updated={metadata.last_updated.isoformat()}"
    )


def main() -> None:
    """Run the explicit live verification for all configured FRED series."""
    settings = load_settings()
    if settings.fred_api_key is None:
        raise RuntimeError("FRED_API_KEY is required for live verification")

    service = FredMacroeconomicIngestionService(
        FredClient(settings.fred_api_key)
    )
    observation_start = date.today() - timedelta(days=730)
    results = service.ingest_initial_series(observation_start=observation_start)

    if tuple(results) != INITIAL_FRED_SERIES_IDS:
        raise RuntimeError("FRED live results do not match configured series order")
    for series_id in INITIAL_FRED_SERIES_IDS:
        _verify_series(series_id, results[series_id])

    print(
        "FRED live verification passed for "
        f"{len(results)}/{len(INITIAL_FRED_SERIES_IDS)} configured series."
    )


if __name__ == "__main__":
    main()
