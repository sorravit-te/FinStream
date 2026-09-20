"""Cross-source validation of the shared Bronze storage contract."""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock

import pyarrow as pa

import finstream.bronze.json_storage as bronze_json_storage
import finstream.bronze.models as bronze_models
import finstream.bronze.parquet_storage as bronze_parquet_storage
import finstream.bronze.paths as bronze_paths
from finstream.bronze.json_storage import read_raw_json
from finstream.bronze.parquet_storage import read_parquet
from finstream.bronze.paths import format_bronze_run_id
from finstream.fred.bronze import (
    FRED_BRONZE_SOURCE,
    FRED_SERIES_METADATA_BRONZE_DATASET,
    FRED_SERIES_METADATA_SCHEMA,
    FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
    FRED_SERIES_OBSERVATIONS_SCHEMA,
)
from finstream.fred.client import FredClient
from finstream.fred.ingestion import FredMacroeconomicIngestionService
from finstream.fred.parsing import parse_series_metadata, parse_series_observations
from finstream.market.bronze import (
    DAILY_MARKET_PRICE_SCHEMA,
    MARKET_BRONZE_DATASET,
    MARKET_BRONZE_SOURCE,
)
from finstream.market.ingestion import MarketIngestionService
from finstream.market.parsing import parse_daily_time_series
from finstream.market.twelve_data import TwelveDataClient
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_COMPANY_FACTS_SCHEMA,
    SEC_SUBMISSIONS_BRONZE_DATASET,
    SEC_SUBMISSIONS_SCHEMA,
)
from finstream.sec.edgar import SecEdgarClient
from finstream.sec.ingestion import SecFinancialIngestionService
from finstream.sec.parsing import parse_company_facts, parse_submissions


_RUN_AT = datetime(
    2026,
    8,
    30,
    19,
    0,
    0,
    123456,
    tzinfo=timezone(timedelta(hours=7)),
)
_CANONICAL_RUN_AT = datetime(
    2026,
    8,
    30,
    12,
    0,
    0,
    123456,
    tzinfo=timezone.utc,
)
_RUN_ID = "20260830T120000123456Z"
_CIK = "0000320193"
_SERIES_ID = "DFF"
_FORBIDDEN_LOCATION_COLUMNS = {"source", "dataset", "run_id", "ingested_at"}


def _market_payload(*, empty: bool = False) -> dict[str, object]:
    values = []
    if not empty:
        values.append(
            {
                "datetime": "2026-08-28",
                "open": "150.100000000000000001",
                "high": "155.500000000000000001",
                "low": "149.250000000000000001",
                "close": "153.330000000000000001",
                "volume": "1234567",
            }
        )
    return {"meta": {"symbol": "AAPL", "interval": "1day"}, "values": values}


def _submissions_payload(*, empty: bool = False) -> dict[str, object]:
    fields: dict[str, list[object]] = {
        "accessionNumber": [],
        "filingDate": [],
        "reportDate": [],
        "acceptanceDateTime": [],
        "act": [],
        "form": [],
        "fileNumber": [],
        "filmNumber": [],
        "items": [],
        "size": [],
        "isXBRL": [],
        "isInlineXBRL": [],
        "primaryDocument": [],
        "primaryDocDescription": [],
    }
    if not empty:
        row = {
            "accessionNumber": "0000001234-26-123456",
            "filingDate": "2026-08-01",
            "reportDate": "2026-06-30",
            "acceptanceDateTime": "2026-08-01T12:01:02+02:00",
            "act": "34",
            "form": "10-Q",
            "fileNumber": "001-36743",
            "filmNumber": "261234567",
            "items": "",
            "size": 123456,
            "isXBRL": 1,
            "isInlineXBRL": 1,
            "primaryDocument": "form10-q.htm",
            "primaryDocDescription": "FORM 10-Q",
        }
        for field_name, value in row.items():
            fields[field_name].append(value)
    return {
        "cik": "320193",
        "name": "Apple Inc.",
        "filings": {"recent": fields, "files": []},
    }


def _company_facts_payload(*, empty: bool = False) -> dict[str, object]:
    facts: dict[str, object] = {}
    if not empty:
        facts = {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "label": "Revenue",
                    "description": "Revenue from contracts",
                    "units": {
                        "USD": [
                            {
                                "start": "2025-01-01",
                                "end": "2025-03-31",
                                "val": 123456,
                                "accn": "0000001234-25-000057",
                                "fy": 2025,
                                "fp": "Q2",
                                "form": "10-Q",
                                "filed": "2025-05-02",
                                "frame": "CY2025Q1",
                            }
                        ]
                    },
                }
            }
        }
    return {"cik": 320193, "entityName": "Apple Inc.", "facts": facts}


def _fred_metadata_payload() -> dict[str, object]:
    return {
        "seriess": [
            {
                "id": _SERIES_ID,
                "realtime_start": "2026-08-01",
                "realtime_end": "2026-08-29",
                "title": "Federal Funds Effective Rate",
                "observation_start": "1954-07-01",
                "observation_end": "2026-08-28",
                "frequency": "Daily",
                "frequency_short": "D",
                "units": "Percent",
                "units_short": "%",
                "seasonal_adjustment": "Not Seasonally Adjusted",
                "seasonal_adjustment_short": "NSA",
                "last_updated": "2026-08-29 19:00:00+07:00",
                "popularity": 99,
                "notes": None,
            }
        ]
    }


def _fred_observations_payload(*, empty: bool = False) -> dict[str, object]:
    observations = []
    if not empty:
        observations = [
            {
                "realtime_start": "2026-08-29",
                "realtime_end": "2026-08-29",
                "date": "2026-08-28",
                "value": "4.33",
            },
            {
                "realtime_start": "2026-08-01",
                "realtime_end": "2026-08-29",
                "date": "2026-08-27",
                "value": ".",
            },
        ]
    return {"observations": observations}


def test_representative_cross_source_ingestion_is_traceable_and_reprocessable(
    tmp_path: Path,
) -> None:
    bronze_root = tmp_path / "bronze"
    market_payload = _market_payload()
    submissions_payload = _submissions_payload()
    facts_payload = _company_facts_payload()
    metadata_payload = _fred_metadata_payload()
    observations_payload = _fred_observations_payload()

    market_client = Mock(spec=TwelveDataClient)
    market_client.fetch_daily_time_series.return_value = market_payload
    sec_client = Mock(spec=SecEdgarClient)
    sec_client.fetch_submissions.return_value = submissions_payload
    sec_client.fetch_company_facts.return_value = facts_payload
    fred_client = Mock(spec=FredClient)
    fred_client.fetch_series.return_value = metadata_payload
    fred_client.fetch_series_observations.return_value = observations_payload

    market = MarketIngestionService(market_client).ingest_symbol_to_bronze(
        "AAPL",
        run_at=_RUN_AT,
        bronze_root=bronze_root,
    )
    sec = SecFinancialIngestionService(
        sec_client,
        request_delay_seconds=0,
    ).ingest_company_to_bronze(
        _CIK,
        run_at=_RUN_AT,
        bronze_root=bronze_root,
    )
    fred = FredMacroeconomicIngestionService(
        fred_client
    ).ingest_series_to_bronze(
        _SERIES_ID,
        run_at=_RUN_AT,
        bronze_root=bronze_root,
    )

    datasets = [
        (
            market,
            MARKET_BRONZE_SOURCE,
            MARKET_BRONZE_DATASET,
            market_payload,
            DAILY_MARKET_PRICE_SCHEMA,
            "AAPL",
        ),
        (
            sec.submissions,
            SEC_BRONZE_SOURCE,
            SEC_SUBMISSIONS_BRONZE_DATASET,
            submissions_payload,
            SEC_SUBMISSIONS_SCHEMA,
            _CIK,
        ),
        (
            sec.company_facts,
            SEC_BRONZE_SOURCE,
            SEC_COMPANY_FACTS_BRONZE_DATASET,
            facts_payload,
            SEC_COMPANY_FACTS_SCHEMA,
            _CIK,
        ),
        (
            fred.metadata,
            FRED_BRONZE_SOURCE,
            FRED_SERIES_METADATA_BRONZE_DATASET,
            metadata_payload,
            FRED_SERIES_METADATA_SCHEMA,
            _SERIES_ID,
        ),
        (
            fred.observations,
            FRED_BRONZE_SOURCE,
            FRED_SERIES_OBSERVATIONS_BRONZE_DATASET,
            observations_payload,
            FRED_SERIES_OBSERVATIONS_SCHEMA,
            _SERIES_ID,
        ),
    ]

    artifact_paths: list[Path] = []
    directories: set[Path] = set()
    tables: dict[tuple[str, str], pa.Table] = {}
    for result, source, dataset, payload, schema, entity in datasets:
        location = result.location
        expected_run_id = format_bronze_run_id(_RUN_AT, entity=entity)
        expected_directory = (
            bronze_root
            / source
            / dataset
            / "ingestion_date=2026-08-30"
            / f"run_id={expected_run_id}"
        )
        assert location.metadata.source == source
        assert location.metadata.dataset == dataset
        assert location.metadata.ingested_at == _CANONICAL_RUN_AT
        assert location.metadata.entity == entity
        assert location.metadata.run_id == expected_run_id
        assert location.directory == expected_directory
        assert result.raw_json_path == expected_directory / "payload.json"
        assert result.parquet_path == expected_directory / "data.parquet"
        assert read_raw_json(location) == payload

        table = read_parquet(location)
        assert table.schema.equals(schema)
        assert table.num_rows == result.record_count
        assert _FORBIDDEN_LOCATION_COLUMNS.isdisjoint(table.schema.names)
        assert table.schema.names == schema.names
        tables[(source, dataset)] = table
        directories.add(location.directory)
        artifact_paths.extend([result.raw_json_path, result.parquet_path])

    assert len(datasets) == 5
    assert len(directories) == 5
    assert len(artifact_paths) == 10
    assert all(path.is_file() for path in artifact_paths)
    assert {path.name for path in artifact_paths} == {"payload.json", "data.parquet"}
    assert {result.location.metadata.run_id for result, *_ in datasets} == {
        format_bronze_run_id(_RUN_AT, entity="AAPL"),
        format_bronze_run_id(_RUN_AT, entity=_CIK),
        format_bronze_run_id(_RUN_AT, entity=_SERIES_ID),
    }

    assert isinstance(
        tables[(MARKET_BRONZE_SOURCE, MARKET_BRONZE_DATASET)]
        .column("open")[0]
        .as_py(),
        Decimal,
    )
    facts_table = tables[(SEC_BRONZE_SOURCE, SEC_COMPANY_FACTS_BRONZE_DATASET)]
    assert isinstance(facts_table.column("value")[0].as_py(), Decimal)
    assert facts_table.column("accession_number").to_pylist() == [
        "0000001234-25-000057"
    ]
    fred_table = tables[
        (FRED_BRONZE_SOURCE, FRED_SERIES_OBSERVATIONS_BRONZE_DATASET)
    ]
    assert fred_table.column("value").to_pylist() == [
        Decimal("4.330000000000000000000000000000"),
        None,
    ]
    assert fred_table.column("realtime_start").to_pylist() == [
        date(2026, 8, 29),
        date(2026, 8, 1),
    ]
    assert fred_table.num_rows == 2

    request_counts = (
        market_client.fetch_daily_time_series.call_count,
        sec_client.fetch_submissions.call_count,
        sec_client.fetch_company_facts.call_count,
        fred_client.fetch_series.call_count,
        fred_client.fetch_series_observations.call_count,
    )
    assert parse_daily_time_series(
        read_raw_json(market.location),
        expected_symbol="AAPL",
    ) == parse_daily_time_series(market_payload, expected_symbol="AAPL")
    assert parse_submissions(
        read_raw_json(sec.submissions.location),
        expected_cik=_CIK,
    ) == parse_submissions(submissions_payload, expected_cik=_CIK)
    assert parse_company_facts(
        read_raw_json(sec.company_facts.location),
        expected_cik=_CIK,
    ) == parse_company_facts(facts_payload, expected_cik=_CIK)
    assert parse_series_metadata(
        read_raw_json(fred.metadata.location),
        expected_series_id=_SERIES_ID,
    ) == parse_series_metadata(metadata_payload, expected_series_id=_SERIES_ID)
    assert parse_series_observations(
        read_raw_json(fred.observations.location),
        expected_series_id=_SERIES_ID,
    ) == parse_series_observations(
        observations_payload,
        expected_series_id=_SERIES_ID,
    )
    assert request_counts == (
        market_client.fetch_daily_time_series.call_count,
        sec_client.fetch_submissions.call_count,
        sec_client.fetch_company_facts.call_count,
        fred_client.fetch_series.call_count,
        fred_client.fetch_series_observations.call_count,
    )


def test_zero_row_datasets_share_the_full_schema_artifact_contract(
    tmp_path: Path,
) -> None:
    bronze_root = tmp_path / "bronze"
    market_payload = _market_payload(empty=True)
    submissions_payload = _submissions_payload(empty=True)
    facts_payload = _company_facts_payload(empty=True)
    observations_payload = _fred_observations_payload(empty=True)

    market_client = Mock(spec=TwelveDataClient)
    market_client.fetch_daily_time_series.return_value = market_payload
    sec_client = Mock(spec=SecEdgarClient)
    sec_client.fetch_submissions.return_value = submissions_payload
    sec_client.fetch_company_facts.return_value = facts_payload
    fred_client = Mock(spec=FredClient)
    fred_client.fetch_series_observations.return_value = observations_payload

    results = [
        (
            MarketIngestionService(market_client).ingest_symbol_to_bronze(
                "AAPL",
                run_at=_RUN_AT,
                bronze_root=bronze_root,
            ),
            market_payload,
            DAILY_MARKET_PRICE_SCHEMA,
        ),
        (
            SecFinancialIngestionService(
                sec_client,
                request_delay_seconds=0,
            ).ingest_submissions_to_bronze(
                _CIK,
                run_at=_RUN_AT,
                bronze_root=bronze_root,
            ),
            submissions_payload,
            SEC_SUBMISSIONS_SCHEMA,
        ),
        (
            SecFinancialIngestionService(
                sec_client,
                request_delay_seconds=0,
            ).ingest_company_facts_to_bronze(
                _CIK,
                run_at=_RUN_AT,
                bronze_root=bronze_root,
            ),
            facts_payload,
            SEC_COMPANY_FACTS_SCHEMA,
        ),
        (
            FredMacroeconomicIngestionService(
                fred_client
            ).ingest_observations_to_bronze(
                _SERIES_ID,
                run_at=_RUN_AT,
                bronze_root=bronze_root,
            ),
            observations_payload,
            FRED_SERIES_OBSERVATIONS_SCHEMA,
        ),
    ]

    for result, payload, schema in results:
        table = read_parquet(result.location)
        assert result.record_count == 0
        assert read_raw_json(result.location) == payload
        assert table.num_rows == 0
        assert table.schema.equals(schema)
        assert result.raw_json_path.name == "payload.json"
        assert result.parquet_path.name == "data.parquet"


def test_generic_bronze_modules_do_not_depend_on_provider_packages() -> None:
    provider_prefixes = (
        "finstream.market",
        "finstream.sec",
        "finstream.fred",
    )
    modules = (
        bronze_json_storage,
        bronze_models,
        bronze_parquet_storage,
        bronze_paths,
    )

    for module in modules:
        for value in vars(module).values():
            referenced_module = (
                value.__name__
                if isinstance(value, ModuleType)
                else getattr(value, "__module__", "")
            )
            assert not referenced_module.startswith(provider_prefixes)
