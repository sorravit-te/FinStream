"""Prepare deterministic source-aligned data for the CI dbt integration check.

This script only accepts the CI database selected through
``FINSTREAM_TEST_POSTGRES_DSN``. It creates the existing source schema and
loads a compact, provider-free fixture that exercises market, financial, and
current-vintage macro paths before ``dbt build``.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg

from finstream.database.schema import ensure_source_schema


_CI_DATABASE_NAME = "finstream_test"
_MARKET_RUN_ID = "20241231T120000000000Z"
_SEC_SUBMISSIONS_RUN_ID = "20241231T120001000000Z"
_SEC_FACTS_RUN_ID = "20241231T120002000000Z"
_FRED_METADATA_RUN_ID = "20241231T120003000000Z"
_FRED_OBSERVATIONS_RUN_ID = "20241231T120004000000Z"


def _test_dsn() -> str:
    dsn = os.environ.get("FINSTREAM_TEST_POSTGRES_DSN")
    if dsn is None or not dsn.strip():
        raise RuntimeError("FINSTREAM_TEST_POSTGRES_DSN must be configured")
    return dsn.strip()


def _insert_fixture(connection: psycopg.Connection) -> None:
    ingested_at = datetime(2024, 12, 31, 12, 0, tzinfo=timezone.utc)
    runs = (
        (
            "twelve_data",
            "daily_market_prices",
            _MARKET_RUN_ID,
            ingested_at,
            "ci://market/payload.json",
            "ci://market/data.parquet",
            2,
        ),
        (
            "sec_edgar",
            "submissions",
            _SEC_SUBMISSIONS_RUN_ID,
            ingested_at,
            "ci://sec/submissions/payload.json",
            "ci://sec/submissions/data.parquet",
            1,
        ),
        (
            "sec_edgar",
            "company_facts",
            _SEC_FACTS_RUN_ID,
            ingested_at,
            "ci://sec/company-facts/payload.json",
            "ci://sec/company-facts/data.parquet",
            2,
        ),
        (
            "fred",
            "series_metadata",
            _FRED_METADATA_RUN_ID,
            ingested_at,
            "ci://fred/metadata/payload.json",
            "ci://fred/metadata/data.parquet",
            1,
        ),
        (
            "fred",
            "series_observations",
            _FRED_OBSERVATIONS_RUN_ID,
            ingested_at,
            "ci://fred/observations/payload.json",
            "ci://fred/observations/data.parquet",
            2,
        ),
    )

    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE source_data.ingestion_runs CASCADE")
        cursor.executemany(
            """INSERT INTO source_data.ingestion_runs (
                   source, dataset, run_id, ingested_at, raw_json_path,
                   parquet_path, record_count
               ) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            runs,
        )
        cursor.executemany(
            """INSERT INTO source_data.market_daily_prices (
                   source, dataset, run_id, source_row_number, symbol,
                   trading_date, open, high, low, close, volume
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                (
                    "twelve_data", "daily_market_prices", _MARKET_RUN_ID, 0,
                    "AAPL", date(2024, 12, 30), Decimal("100"),
                    Decimal("102"), Decimal("99"), Decimal("101"), 1000,
                ),
                (
                    "twelve_data", "daily_market_prices", _MARKET_RUN_ID, 1,
                    "AAPL", date(2024, 12, 31), Decimal("101"),
                    Decimal("104"), Decimal("100"), Decimal("103"), 1100,
                ),
            ),
        )
        cursor.execute(
            """INSERT INTO source_data.sec_submissions (
                   source, dataset, run_id, source_row_number, cik,
                   company_name, accession_number, filing_date, report_date,
                   acceptance_datetime, form, act, file_number, film_number,
                   items, size, is_xbrl, is_inline_xbrl, primary_document,
                   primary_doc_description
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s, %s, %s, %s)""",
            (
                "sec_edgar", "submissions", _SEC_SUBMISSIONS_RUN_ID, 0,
                "0000320193", "Apple Inc.", "0000320193-25-000001",
                date(2025, 2, 1), date(2024, 12, 31), ingested_at, "10-K",
                None, None, None, None, 1, True, True, "form10k.htm", None,
            ),
        )
        cursor.executemany(
            """INSERT INTO source_data.sec_company_facts (
                   source, dataset, run_id, source_row_number, cik, entity_name,
                   taxonomy, concept, label, description, unit, value, start_date,
                   end_date, accession_number, fiscal_year, fiscal_period, form,
                   filed_date, frame
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s, %s, %s, %s)""",
            (
                (
                    "sec_edgar", "company_facts", _SEC_FACTS_RUN_ID, 0,
                    "0000320193", "Apple Inc.", "us-gaap",
                    "RevenueFromContractWithCustomerExcludingAssessedTax",
                    "Revenue", None, "USD", Decimal("100"),
                    date(2023, 1, 1), date(2023, 12, 31),
                    "0000320193-24-000001", 2023, "FY", "10-K",
                    date(2024, 2, 1), None,
                ),
                (
                    "sec_edgar", "company_facts", _SEC_FACTS_RUN_ID, 1,
                    "0000320193", "Apple Inc.", "us-gaap",
                    "RevenueFromContractWithCustomerExcludingAssessedTax",
                    "Revenue", None, "USD", Decimal("120"),
                    date(2024, 1, 1), date(2024, 12, 31),
                    "0000320193-25-000001", 2024, "FY", "10-K",
                    date(2025, 2, 1), None,
                ),
            ),
        )
        cursor.execute(
            """INSERT INTO source_data.fred_series_metadata (
                   source, dataset, run_id, source_row_number, series_id,
                   realtime_start, realtime_end, title, observation_start,
                   observation_end, frequency, frequency_short, units,
                   units_short, seasonal_adjustment, seasonal_adjustment_short,
                   last_updated, popularity, notes
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s, %s, %s)""",
            (
                "fred", "series_metadata", _FRED_METADATA_RUN_ID, 0, "DFF",
                date(2024, 12, 1), date(2024, 12, 31), "Federal Funds Rate",
                date(2024, 12, 1), date(2024, 12, 31), "Daily", "D", "Percent",
                "%", "Not Seasonally Adjusted", "NSA", ingested_at, 1, None,
            ),
        )
        cursor.executemany(
            """INSERT INTO source_data.fred_series_observations (
                   source, dataset, run_id, source_row_number, series_id,
                   realtime_start, realtime_end, observation_date, value
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                (
                    "fred", "series_observations", _FRED_OBSERVATIONS_RUN_ID,
                    0, "DFF", date(2024, 12, 1), date(2024, 12, 31),
                    date(2024, 12, 1), Decimal("4.50"),
                ),
                (
                    "fred", "series_observations", _FRED_OBSERVATIONS_RUN_ID,
                    1, "DFF", date(2024, 12, 1), date(2024, 12, 31),
                    date(2024, 12, 31), Decimal("4.60"),
                ),
            ),
        )


def main() -> None:
    connection = psycopg.connect(_test_dsn(), autocommit=False)
    try:
        if connection.info.dbname != _CI_DATABASE_NAME:
            raise RuntimeError(
                f"CI fixture setup requires database {_CI_DATABASE_NAME!r}, "
                f"got {connection.info.dbname!r}"
            )
        ensure_source_schema(connection)
        _insert_fixture(connection)
        connection.commit()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
