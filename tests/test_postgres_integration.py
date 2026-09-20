"""Opt-in real PostgreSQL validation for the complete Step 7 loading path."""

import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from finstream.bronze.json_storage import write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import write_parquet
from finstream.database.schema import ensure_source_schema
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
    FredBronzeDatasetResult,
    FredSeriesBronzeResult,
    fred_series_metadata_to_table,
    fred_series_observations_to_table,
)
from finstream.fred.models import (
    FredObservation,
    FredSeriesMetadata,
    FredSeriesObservations,
)
from finstream.fred.postgres import load_fred_series_bronze_to_postgres
from finstream.market.bronze import (
    MARKET_BRONZE_DATASET,
    MARKET_BRONZE_SOURCE,
    MarketBronzeResult,
    daily_market_prices_to_table,
)
from finstream.market.models import DailyMarketPrice
from finstream.market.postgres import load_market_bronze_to_postgres
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_SUBMISSIONS_BRONZE_DATASET,
    SecBronzeDatasetResult,
    SecCompanyBronzeResult,
    sec_company_facts_to_table,
    sec_submissions_to_table,
)
from finstream.sec.models import (
    SecCompanyFacts,
    SecFilingMetadata,
    SecFinancialFact,
    SecSubmissions,
)
from finstream.sec.postgres import load_sec_company_bronze_to_postgres


_CIK = "0000320193"
_SERIES_ID = "DFF"
_MARKET_RUN_AT = datetime(2099, 12, 31, 23, 59, 50, 100001, tzinfo=timezone.utc)
_SEC_RUN_AT = datetime(2099, 12, 31, 23, 59, 51, 100002, tzinfo=timezone.utc)
_FRED_RUN_AT = datetime(2099, 12, 31, 23, 59, 52, 100003, tzinfo=timezone.utc)


def _test_dsn() -> str:
    dsn = os.environ.get("FINSTREAM_TEST_POSTGRES_DSN")
    if dsn is None or not dsn.strip():
        pytest.skip("FINSTREAM_TEST_POSTGRES_DSN is not configured")
    return dsn.strip()


def _location(
    bronze_root: Path,
    *,
    source: str,
    dataset: str,
    run_at: datetime,
    entity: str | None = None,
) -> BronzeRunLocation:
    return BronzeRunLocation.from_run(
        root=bronze_root,
        source=source,
        dataset=dataset,
        ingested_at=run_at,
        entity=entity,
    )


def _persist_dataset(
    location: BronzeRunLocation,
    table: object,
) -> tuple[Path, Path]:
    raw_path = write_raw_json(location, {"integration_test": True})
    parquet_artifact = write_parquet(location, table)  # type: ignore[arg-type]
    return raw_path, parquet_artifact


def _market_result(bronze_root: Path) -> MarketBronzeResult:
    records = [
        DailyMarketPrice(
            symbol="AAPL",
            trading_date=date(2099, 12, 30),
            open=Decimal("100.123456789012345678"),
            high=Decimal("105.123456789012345678"),
            low=Decimal("99.123456789012345678"),
            close=Decimal("104.123456789012345678"),
            volume=123456,
        ),
        DailyMarketPrice(
            symbol="AAPL",
            trading_date=date(2099, 12, 29),
            open=Decimal("90.000000000000000001"),
            high=Decimal("95.000000000000000001"),
            low=Decimal("89.000000000000000001"),
            close=Decimal("94.000000000000000001"),
            volume=None,
        ),
    ]
    table = daily_market_prices_to_table(records)
    location = _location(
        bronze_root,
        source=MARKET_BRONZE_SOURCE,
        dataset=MARKET_BRONZE_DATASET,
        run_at=_MARKET_RUN_AT,
        entity="AAPL",
    )
    raw_path, parquet_artifact = _persist_dataset(location, table)
    return MarketBronzeResult(
        symbol="AAPL",
        location=location,
        raw_json_path=raw_path,
        parquet_path=parquet_artifact,
        record_count=table.num_rows,
    )


def _sec_result(bronze_root: Path) -> SecCompanyBronzeResult:
    submissions = SecSubmissions(
        cik=_CIK,
        company_name="Apple Inc.",
        filings=(
            SecFilingMetadata(
                cik=_CIK,
                accession_number="0000001234-99-000001",
                filing_date=date(2099, 12, 20),
                report_date=None,
                acceptance_datetime=datetime(
                    2099,
                    12,
                    20,
                    12,
                    30,
                    tzinfo=timezone(timedelta(hours=7)),
                ),
                form="10-Q",
                act=None,
                file_number=None,
                film_number=None,
                items=None,
                size=100,
                is_xbrl=True,
                is_inline_xbrl=True,
                primary_document="form10-q.htm",
                primary_doc_description=None,
            ),
        ),
    )
    facts = SecCompanyFacts(
        cik=_CIK,
        entity_name="Apple Inc.",
        facts=(
            SecFinancialFact(
                cik=_CIK,
                taxonomy="us-gaap",
                concept="RevenueFromContractWithCustomerExcludingAssessedTax",
                label="Revenue",
                description=None,
                unit="USD",
                value=Decimal("123456.123456789012345678901234567890"),
                start_date=date(2099, 1, 1),
                end_date=date(2099, 3, 31),
                accession_number="0000001234-99-000001",
                fiscal_year=2099,
                fiscal_period="Q2",
                form="10-Q",
                filed_date=date(2099, 5, 2),
                frame="CY2099Q1",
            ),
            SecFinancialFact(
                cik=_CIK,
                taxonomy="us-gaap",
                concept="Assets",
                label=None,
                description=None,
                unit="USD",
                value=Decimal("987654.000000000000000000000000000001"),
                start_date=None,
                end_date=date(2099, 6, 30),
                accession_number="0000001234-99-000002",
                fiscal_year=None,
                fiscal_period=None,
                form="10-Q",
                filed_date=date(2099, 8, 1),
                frame=None,
            ),
        ),
    )
    submissions_table = sec_submissions_to_table(submissions)
    facts_table = sec_company_facts_to_table(facts)
    submissions_location = _location(
        bronze_root,
        source=SEC_BRONZE_SOURCE,
        dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
        run_at=_SEC_RUN_AT,
        entity=_CIK,
    )
    facts_location = _location(
        bronze_root,
        source=SEC_BRONZE_SOURCE,
        dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
        run_at=_SEC_RUN_AT,
        entity=_CIK,
    )
    submissions_raw, submissions_parquet = _persist_dataset(
        submissions_location,
        submissions_table,
    )
    facts_raw, facts_parquet = _persist_dataset(facts_location, facts_table)
    return SecCompanyBronzeResult(
        cik=_CIK,
        submissions=SecBronzeDatasetResult(
            cik=_CIK,
            location=submissions_location,
            raw_json_path=submissions_raw,
            parquet_path=submissions_parquet,
            record_count=submissions_table.num_rows,
        ),
        company_facts=SecBronzeDatasetResult(
            cik=_CIK,
            location=facts_location,
            raw_json_path=facts_raw,
            parquet_path=facts_parquet,
            record_count=facts_table.num_rows,
        ),
    )


def _fred_result(bronze_root: Path) -> FredSeriesBronzeResult:
    metadata = FredSeriesMetadata(
        series_id=_SERIES_ID,
        realtime_start=date(2099, 12, 1),
        realtime_end=date(2099, 12, 31),
        title="Federal Funds Effective Rate",
        observation_start=date(1954, 7, 1),
        observation_end=date(2099, 12, 30),
        frequency="Daily",
        frequency_short="D",
        units="Percent",
        units_short="%",
        seasonal_adjustment="Not Seasonally Adjusted",
        seasonal_adjustment_short="NSA",
        last_updated=datetime(
            2099,
            12,
            30,
            19,
            tzinfo=timezone(timedelta(hours=7)),
        ),
        popularity=99,
        notes=None,
    )
    observations = FredSeriesObservations(
        series_id=_SERIES_ID,
        observations=(
            FredObservation(
                series_id=_SERIES_ID,
                realtime_start=date(2099, 12, 30),
                realtime_end=date(2099, 12, 31),
                observation_date=date(2099, 12, 30),
                value=Decimal("4.330000000000000000000000000001"),
            ),
            FredObservation(
                series_id=_SERIES_ID,
                realtime_start=date(2099, 12, 1),
                realtime_end=date(2099, 12, 31),
                observation_date=date(2099, 12, 29),
                value=None,
            ),
        ),
    )
    metadata_table = fred_series_metadata_to_table(metadata)
    observations_table = fred_series_observations_to_table(observations)
    metadata_location = _location(
        bronze_root,
        source=FRED_BRONZE_SOURCE,
        dataset=FRED_SERIES_METADATA_BRONZE_DATASET,
        run_at=_FRED_RUN_AT,
        entity=_SERIES_ID,
    )
    observations_location = _location(
        bronze_root,
        source=FRED_BRONZE_SOURCE,
        dataset=FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
        run_at=_FRED_RUN_AT,
        entity=_SERIES_ID,
    )
    metadata_raw, metadata_parquet = _persist_dataset(
        metadata_location,
        metadata_table,
    )
    observations_raw, observations_parquet = _persist_dataset(
        observations_location,
        observations_table,
    )
    return FredSeriesBronzeResult(
        series_id=_SERIES_ID,
        metadata=FredBronzeDatasetResult(
            series_id=_SERIES_ID,
            location=metadata_location,
            raw_json_path=metadata_raw,
            parquet_path=metadata_parquet,
            record_count=metadata_table.num_rows,
        ),
        observations=FredBronzeDatasetResult(
            series_id=_SERIES_ID,
            location=observations_location,
            raw_json_path=observations_raw,
            parquet_path=observations_parquet,
            record_count=observations_table.num_rows,
        ),
    )


def _registry_identities(
    market: MarketBronzeResult,
    sec: SecCompanyBronzeResult,
    fred: FredSeriesBronzeResult,
) -> tuple[tuple[str, str, str], ...]:
    results = (
        market,
        sec.submissions,
        sec.company_facts,
        fred.metadata,
        fred.observations,
    )
    return tuple(
        (
            result.location.metadata.source,
            result.location.metadata.dataset,
            result.location.metadata.run_id,
        )
        for result in results
    )


def _registry_where() -> str:
    clause = "(source = %s AND dataset = %s AND run_id = %s)"
    return " OR ".join([clause] * 5)


def _registry_parameters(
    identities: tuple[tuple[str, str, str], ...],
) -> tuple[str, ...]:
    return tuple(value for identity in identities for value in identity)


@pytest.mark.integration
def test_real_postgres_cross_source_loading_and_constraints(tmp_path: Path) -> None:
    test_dsn = _test_dsn()
    market = _market_result(tmp_path / "market")
    sec = _sec_result(tmp_path / "sec")
    fred = _fred_result(tmp_path / "fred")
    identities = _registry_identities(market, sec, fred)
    connection = psycopg.connect(test_dsn, autocommit=False)

    try:
        ensure_source_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_setting('server_version')")
            assert isinstance(cursor.fetchone()[0], str)

        assert load_market_bronze_to_postgres(connection, market) == 2
        sec_counts = load_sec_company_bronze_to_postgres(connection, sec)
        assert sec_counts.submissions_loaded == 1
        assert sec_counts.company_facts_loaded == 2
        fred_counts = load_fred_series_bronze_to_postgres(connection, fred)
        assert fred_counts.metadata_loaded == 1
        assert fred_counts.observations_loaded == 2

        with connection.cursor() as cursor:
            cursor.execute(
                f"""SELECT source, dataset, run_id, record_count
                    FROM source_data.ingestion_runs
                    WHERE {_registry_where()}
                    ORDER BY source, dataset""",
                _registry_parameters(identities),
            )
            registry_rows = cursor.fetchall()
            assert len(registry_rows) == 5
            assert {(row[0], row[1]) for row in registry_rows} == {
                ("twelve_data", "daily_market_prices"),
                ("sec_edgar", "submissions"),
                ("sec_edgar", "company_facts"),
                ("fred", "series_metadata"),
                ("fred", "series_observations"),
            }

            cursor.execute(
                """SELECT source, dataset, run_id, source_row_number,
                           open, volume
                    FROM source_data.market_daily_prices
                    WHERE run_id = %s
                    ORDER BY source_row_number""",
                (market.location.metadata.run_id,),
            )
            market_rows = cursor.fetchall()
            assert [row[3] for row in market_rows] == [0, 1]
            assert market_rows[0][0:3] == (
                "twelve_data",
                "daily_market_prices",
                market.location.metadata.run_id,
            )
            assert market_rows[0][4] == Decimal("100.123456789012345678")
            assert [row[5] for row in market_rows] == [123456, None]

            cursor.execute(
                """SELECT source_row_number, acceptance_datetime, report_date
                    FROM source_data.sec_submissions
                    WHERE run_id = %s""",
                (sec.submissions.location.metadata.run_id,),
            )
            submission_row = cursor.fetchone()
            assert submission_row[0] == 0
            assert submission_row[1].tzinfo is not None
            assert submission_row[2] is None

            cursor.execute(
                """SELECT source_row_number, concept, value, start_date, frame
                    FROM source_data.sec_company_facts
                    WHERE run_id = %s
                    ORDER BY source_row_number""",
                (sec.company_facts.location.metadata.run_id,),
            )
            fact_rows = cursor.fetchall()
            assert [row[0] for row in fact_rows] == [0, 1]
            assert [row[1] for row in fact_rows] == [
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "Assets",
            ]
            assert fact_rows[0][2] == Decimal(
                "123456.123456789012345678901234567890"
            )
            assert fact_rows[1][3:5] == (None, None)

            cursor.execute(
                """SELECT source_row_number, last_updated, notes
                    FROM source_data.fred_series_metadata
                    WHERE run_id = %s""",
                (fred.metadata.location.metadata.run_id,),
            )
            metadata_row = cursor.fetchone()
            assert metadata_row[0] == 0
            assert metadata_row[1].tzinfo is not None
            assert metadata_row[2] is None

            cursor.execute(
                """SELECT source_row_number, realtime_start, realtime_end,
                           observation_date, value
                    FROM source_data.fred_series_observations
                    WHERE run_id = %s
                    ORDER BY source_row_number""",
                (fred.observations.location.metadata.run_id,),
            )
            observation_rows = cursor.fetchall()
            assert [row[0] for row in observation_rows] == [0, 1]
            assert observation_rows[0][1:4] == (
                date(2099, 12, 30),
                date(2099, 12, 31),
                date(2099, 12, 30),
            )
            assert observation_rows[0][4] == Decimal(
                "4.330000000000000000000000000001"
            )
            assert observation_rows[1][4] is None

        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        """INSERT INTO source_data.market_daily_prices (
                               source, dataset, run_id, source_row_number,
                               symbol, trading_date, open, high, low, close, volume
                           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            "twelve_data",
                            "daily_market_prices",
                            "20991231T235959999999Z",
                            0,
                            "AAPL",
                            date(2099, 12, 31),
                            Decimal("1"),
                            Decimal("1"),
                            Decimal("1"),
                            Decimal("1"),
                            1,
                        ),
                    )

        with pytest.raises(psycopg.errors.CheckViolation):
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        """INSERT INTO source_data.market_daily_prices (
                               source, dataset, run_id, source_row_number,
                               symbol, trading_date, open, high, low, close, volume
                           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            "twelve_data",
                            "daily_market_prices",
                            market.location.metadata.run_id,
                            99,
                            "BAD",
                            date(2099, 12, 31),
                            Decimal("10"),
                            Decimal("5"),
                            Decimal("1"),
                            Decimal("2"),
                            1,
                        ),
                    )

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)
    finally:
        connection.rollback()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regclass('source_data.ingestion_runs')")
                if cursor.fetchone()[0] is not None:
                    cursor.execute(
                        f"""SELECT count(*)
                            FROM source_data.ingestion_runs
                            WHERE {_registry_where()}""",
                        _registry_parameters(identities),
                    )
                    assert cursor.fetchone()[0] == 0
        finally:
            connection.rollback()
            connection.close()


@pytest.mark.integration
def test_real_postgres_entity_aware_registry_ids_and_schema_upgrade(
    tmp_path: Path,
) -> None:
    test_dsn = _test_dsn()
    shared_run_at = datetime(2099, 12, 31, 23, 59, 53, 100004, tzinfo=timezone.utc)
    legacy_location = _location(
        tmp_path / "legacy",
        source=MARKET_BRONZE_SOURCE,
        dataset=MARKET_BRONZE_DATASET,
        run_at=shared_run_at,
    )
    aapl_location = _location(
        tmp_path / "aapl",
        source=MARKET_BRONZE_SOURCE,
        dataset=MARKET_BRONZE_DATASET,
        run_at=shared_run_at,
        entity="AAPL",
    )
    msft_location = _location(
        tmp_path / "msft",
        source=MARKET_BRONZE_SOURCE,
        dataset=MARKET_BRONZE_DATASET,
        run_at=shared_run_at,
        entity="MSFT",
    )
    locations = (legacy_location, aapl_location, msft_location)
    connection = psycopg.connect(test_dsn, autocommit=False)

    try:
        ensure_source_schema(connection)
        assert legacy_location.metadata.run_id != aapl_location.metadata.run_id
        assert aapl_location.metadata.run_id != msft_location.metadata.run_id
        assert aapl_location.metadata.ingested_at == msft_location.metadata.ingested_at

        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT pg_get_constraintdef(constraint.oid)
                    FROM pg_constraint AS constraint
                    JOIN pg_class AS relation ON relation.oid = constraint.conrelid
                    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname = 'source_data'
                      AND relation.relname = 'ingestion_runs'
                      AND constraint.conname = 'ck_ingestion_runs_run_id_format'"""
            )
            assert "--entity-" in cursor.fetchone()[0]

            for location in locations:
                metadata = location.metadata
                cursor.execute(
                    """INSERT INTO source_data.ingestion_runs (
                           source, dataset, run_id, ingested_at, raw_json_path,
                           parquet_path, record_count
                       ) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (
                        metadata.source,
                        metadata.dataset,
                        metadata.run_id,
                        metadata.ingested_at,
                        str(location.directory / "payload.json"),
                        str(location.directory / "data.parquet"),
                        0,
                    ),
                )

            cursor.execute(
                """SELECT run_id
                    FROM source_data.ingestion_runs
                    WHERE source = %s AND dataset = %s AND ingested_at = %s
                    ORDER BY run_id""",
                (
                    MARKET_BRONZE_SOURCE,
                    MARKET_BRONZE_DATASET,
                    shared_run_at,
                ),
            )
            assert {row[0] for row in cursor.fetchall()} >= {
                location.metadata.run_id for location in locations
            }
    finally:
        connection.rollback()
        connection.close()
