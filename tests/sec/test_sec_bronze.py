from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, call

import pyarrow as pa
import pytest

import finstream.sec.ingestion as sec_ingestion
from finstream.bronze.json_storage import (
    BronzeRawJsonValidationError,
    read_raw_json,
    write_raw_json,
)
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.recovery import BronzeRecoveryError
from finstream.bronze.parquet_storage import BronzeParquetWriteError, read_parquet
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_COMPANY_FACTS_SCHEMA,
    SEC_SUBMISSIONS_BRONZE_DATASET,
    SEC_SUBMISSIONS_SCHEMA,
    SecBronzeValidationError,
    sec_company_facts_to_table,
    sec_submissions_to_table,
)
from finstream.sec.edgar import SecEdgarClient, SecEdgarError
from finstream.sec.ingestion import SecFinancialIngestionService
from finstream.sec.models import (
    SecCompanyFacts,
    SecFinancialFact,
    SecSubmissions,
)
from finstream.sec.parsing import (
    SecCompanyFactsValidationError,
    SecSubmissionsValidationError,
    parse_company_facts,
    parse_submissions,
)


_RUN_AT = datetime(2026, 8, 30, 12, 0, 0, 123456, tzinfo=timezone.utc)
_CIK = "0000320193"
_RECENT_FIELDS = (
    "accessionNumber",
    "filingDate",
    "reportDate",
    "acceptanceDateTime",
    "act",
    "form",
    "fileNumber",
    "filmNumber",
    "items",
    "size",
    "isXBRL",
    "isInlineXBRL",
    "primaryDocument",
    "primaryDocDescription",
)


def _filing_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
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
    row.update(overrides)
    return row


def _submissions_payload(
    *rows: dict[str, object],
    cik: str = _CIK,
) -> dict:
    recent = {
        field: [row[field] for row in rows]
        for field in _RECENT_FIELDS
    }
    return {
        "cik": str(int(cik)),
        "name": "Apple Inc.",
        "filings": {"recent": recent, "files": []},
    }


def _occurrence(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
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
    value.update(overrides)
    return value


def _company_facts_payload(
    *occurrences: dict[str, object],
    cik: str = _CIK,
) -> dict:
    return {
        "cik": int(cik),
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "label": "Revenue",
                    "description": "Revenue from contracts",
                    "units": {"USD": list(occurrences)},
                }
            }
        }
        if occurrences
        else {},
    }


def _location(tmp_path: Path, dataset: str) -> BronzeRunLocation:
    return BronzeRunLocation.from_run(
        root=tmp_path / "bronze",
        source=SEC_BRONZE_SOURCE,
        dataset=dataset,
        ingested_at=_RUN_AT,
        entity="0000320193",
    )


def test_sec_bronze_locations_use_distinct_datasets_and_same_run_id(
    tmp_path: Path,
) -> None:
    submissions = _location(tmp_path, SEC_SUBMISSIONS_BRONZE_DATASET)
    facts = _location(tmp_path, SEC_COMPANY_FACTS_BRONZE_DATASET)

    assert SEC_BRONZE_SOURCE == "sec_edgar"
    assert SEC_SUBMISSIONS_BRONZE_DATASET == "submissions"
    assert SEC_COMPANY_FACTS_BRONZE_DATASET == "company_facts"
    assert submissions.metadata.run_id == facts.metadata.run_id
    assert submissions.directory != facts.directory


def test_sec_same_timestamp_distinguishes_ciks_and_preserves_company_identity(
    tmp_path: Path,
) -> None:
    msft_cik = "0000789019"
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.side_effect = [
        _submissions_payload(_filing_row()),
        _submissions_payload(_filing_row(), cik=msft_cik),
    ]
    client.fetch_company_facts.side_effect = [
        _company_facts_payload(_occurrence()),
        _company_facts_payload(_occurrence(), cik=msft_cik),
    ]
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    aapl = service.ingest_company_to_bronze(_CIK, run_at=_RUN_AT, bronze_root=tmp_path / "bronze")
    msft = service.ingest_company_to_bronze("789019", run_at=_RUN_AT, bronze_root=tmp_path / "bronze")

    assert aapl.submissions.location.metadata.ingested_at == msft.submissions.location.metadata.ingested_at == _RUN_AT
    assert aapl.submissions.location.metadata.run_id == aapl.company_facts.location.metadata.run_id
    assert aapl.submissions.location.directory != aapl.company_facts.location.directory
    assert aapl.submissions.location.metadata.run_id != msft.submissions.location.metadata.run_id
    assert aapl.submissions.location.directory != msft.submissions.location.directory
    assert msft.cik == msft_cik


def test_submissions_schema_is_exact() -> None:
    assert SEC_SUBMISSIONS_SCHEMA.names == [
        "cik",
        "company_name",
        "accession_number",
        "filing_date",
        "report_date",
        "acceptance_datetime",
        "form",
        "act",
        "file_number",
        "film_number",
        "items",
        "size",
        "is_xbrl",
        "is_inline_xbrl",
        "primary_document",
        "primary_doc_description",
    ]
    expected_types = [
        pa.string(),
        pa.string(),
        pa.string(),
        pa.date32(),
        pa.date32(),
        pa.timestamp("us", tz="UTC"),
        pa.string(),
        pa.string(),
        pa.string(),
        pa.string(),
        pa.string(),
        pa.int64(),
        pa.bool_(),
        pa.bool_(),
        pa.string(),
        pa.string(),
    ]
    assert list(SEC_SUBMISSIONS_SCHEMA.types) == expected_types
    assert [field.nullable for field in SEC_SUBMISSIONS_SCHEMA] == [
        False, False, False, False, True, True, False, True,
        True, True, True, False, False, False, True, True,
    ]


def test_submissions_conversion_preserves_order_and_normalizes_utc() -> None:
    payload = _submissions_payload(
        _filing_row(),
        _filing_row(
            accessionNumber="0000001234-26-654321",
            filingDate="2026-07-01",
        ),
    )
    submissions = parse_submissions(payload, expected_cik=_CIK)

    table = sec_submissions_to_table(submissions)

    assert table.schema.equals(SEC_SUBMISSIONS_SCHEMA)
    assert table.column("accession_number").to_pylist() == [
        "0000001234-26-123456",
        "0000001234-26-654321",
    ]
    assert table.column("cik").to_pylist() == [_CIK, _CIK]
    assert table.column("company_name").to_pylist() == ["Apple Inc.", "Apple Inc."]
    expected_instant = datetime(
        2026,
        8,
        1,
        10,
        1,
        2,
        tzinfo=timezone.utc,
    )
    assert table.column("acceptance_datetime").cast(pa.int64()).to_pylist()[0] == (
        int(expected_instant.timestamp() * 1_000_000)
    )


def test_submissions_conversion_supports_empty_and_rejects_wrong_type() -> None:
    empty = parse_submissions(_submissions_payload(), expected_cik=_CIK)

    table = sec_submissions_to_table(empty)

    assert table.num_rows == 0
    assert table.schema.equals(SEC_SUBMISSIONS_SCHEMA)
    with pytest.raises(SecBronzeValidationError):
        sec_submissions_to_table(object())  # type: ignore[arg-type]


def test_company_facts_schema_is_exact() -> None:
    assert SEC_COMPANY_FACTS_SCHEMA.names == [
        "cik",
        "entity_name",
        "taxonomy",
        "concept",
        "label",
        "description",
        "unit",
        "value",
        "start_date",
        "end_date",
        "accession_number",
        "fiscal_year",
        "fiscal_period",
        "form",
        "filed_date",
        "frame",
    ]
    assert SEC_COMPANY_FACTS_SCHEMA.field("value") == pa.field(
        "value",
        pa.decimal256(76, 30),
        nullable=False,
    )
    assert [field.nullable for field in SEC_COMPANY_FACTS_SCHEMA] == [
        False, False, False, False, True, True, False, False,
        True, False, False, True, True, False, False, True,
    ]


def test_company_facts_conversion_preserves_decimal_order_and_provenance() -> None:
    payload = _company_facts_payload(
        _occurrence(),
        _occurrence(
            val=-25,
            accn="0000001234-25-000058",
            frame=None,
        ),
    )
    company_facts = parse_company_facts(payload, expected_cik=_CIK)

    table = sec_company_facts_to_table(company_facts)

    assert table.schema.equals(SEC_COMPANY_FACTS_SCHEMA)
    assert table.column("value").to_pylist() == [
        Decimal("123456.000000000000000000000000000000"),
        Decimal("-25.000000000000000000000000000000"),
    ]
    assert table.column("accession_number").to_pylist() == [
        "0000001234-25-000057",
        "0000001234-25-000058",
    ]
    assert table.column("taxonomy").to_pylist() == ["us-gaap", "us-gaap"]
    assert table.column("concept").to_pylist() == [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
    ]
    assert table.column("frame").to_pylist() == ["CY2025Q1", None]


def test_company_facts_conversion_supports_empty_and_rejects_large_decimal() -> None:
    empty = parse_company_facts(_company_facts_payload(), expected_cik=_CIK)
    table = sec_company_facts_to_table(empty)
    assert table.num_rows == 0
    assert table.schema.equals(SEC_COMPANY_FACTS_SCHEMA)

    oversized = SecCompanyFacts(
        cik=_CIK,
        entity_name="Apple Inc.",
        facts=(
            SecFinancialFact(
                cik=_CIK,
                taxonomy="us-gaap",
                concept="SourceConcept",
                label=None,
                description=None,
                unit="USD",
                value=Decimal("1e100"),
                start_date=None,
                end_date=date(2025, 3, 31),
                accession_number="0000001234-25-000057",
                fiscal_year=None,
                fiscal_period=None,
                form="8-K",
                filed_date=date(2025, 5, 2),
                frame=None,
            ),
        ),
    )
    with pytest.raises(SecBronzeValidationError):
        sec_company_facts_to_table(oversized)


def test_endpoint_ingestion_preserves_raw_and_reprocesses_without_request(
    tmp_path: Path,
) -> None:
    submissions_payload = _submissions_payload(_filing_row())
    facts_payload = _company_facts_payload(_occurrence())
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = submissions_payload
    client.fetch_company_facts.return_value = facts_payload
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    submissions_result = service.ingest_submissions_to_bronze(
        320193,
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )
    facts_result = service.ingest_company_facts_to_bronze(
        320193,
        run_at=_RUN_AT + timedelta(seconds=1),
        bronze_root=tmp_path / "bronze",
    )

    assert submissions_result.record_count == 1
    assert facts_result.record_count == 1
    assert read_raw_json(submissions_result.location) == submissions_payload
    assert read_raw_json(facts_result.location) == facts_payload
    assert parse_submissions(
        read_raw_json(submissions_result.location),
        expected_cik=_CIK,
    ) == parse_submissions(submissions_payload, expected_cik=_CIK)
    assert parse_company_facts(
        read_raw_json(facts_result.location),
        expected_cik=_CIK,
    ) == parse_company_facts(facts_payload, expected_cik=_CIK)
    assert read_parquet(submissions_result.location).num_rows == 1
    assert read_parquet(facts_result.location).num_rows == 1
    client.fetch_submissions.assert_called_once_with(_CIK)
    client.fetch_company_facts.assert_called_once_with(_CIK)


def test_non_utc_source_timestamp_remains_exact_in_raw_json(tmp_path: Path) -> None:
    payload = _submissions_payload(_filing_row())
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = payload
    result = SecFinancialIngestionService(
        client,
        request_delay_seconds=0,
    ).ingest_submissions_to_bronze(
        320193,
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert (
        read_raw_json(result.location)["filings"]["recent"]["acceptanceDateTime"][0]
        == "2026-08-01T12:01:02+02:00"
    )
    assert read_parquet(result.location).column("acceptance_datetime").type == (
        pa.timestamp("us", tz="UTC")
    )


def test_empty_endpoints_write_zero_row_full_schema_artifacts(tmp_path: Path) -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = _submissions_payload()
    client.fetch_company_facts.return_value = _company_facts_payload()
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    submissions = service.ingest_submissions_to_bronze(
        320193,
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )
    facts = service.ingest_company_facts_to_bronze(
        320193,
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert submissions.record_count == 0
    assert facts.record_count == 0
    assert read_parquet(submissions.location).schema.equals(SEC_SUBMISSIONS_SCHEMA)
    assert read_parquet(facts.location).schema.equals(SEC_COMPANY_FACTS_SCHEMA)


def test_company_bronze_preserves_request_order_pacing_and_four_artifacts(
    tmp_path: Path,
) -> None:
    events: list[object] = []
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.side_effect = lambda cik: (
        events.append(("submissions", cik))
        or _submissions_payload(_filing_row())
    )
    client.fetch_company_facts.side_effect = lambda cik: (
        events.append(("company_facts", cik))
        or _company_facts_payload(_occurrence())
    )
    sleeper = Mock(side_effect=lambda delay: events.append(("sleep", delay)))
    service = SecFinancialIngestionService(client, sleeper=sleeper)

    result = service.ingest_company_to_bronze(
        320193,
        run_at=_RUN_AT,
        bronze_root=tmp_path / "bronze",
    )

    assert events == [
        ("submissions", _CIK),
        ("sleep", 0.125),
        ("company_facts", _CIK),
    ]
    assert result.submissions.location.metadata.run_id == (
        result.company_facts.location.metadata.run_id
    )
    for dataset_result in (result.submissions, result.company_facts):
        assert sorted(
            path.name for path in dataset_result.location.directory.iterdir()
        ) == ["data.parquet", "payload.json"]


def test_same_run_raw_only_sec_datasets_reconstruct_without_requests(
    tmp_path: Path,
) -> None:
    submissions_location = _location(tmp_path, SEC_SUBMISSIONS_BRONZE_DATASET)
    facts_location = _location(tmp_path, SEC_COMPANY_FACTS_BRONZE_DATASET)
    write_raw_json(submissions_location, _submissions_payload(_filing_row()))
    write_raw_json(facts_location, _company_facts_payload(_occurrence()))
    client = Mock(spec=SecEdgarClient)
    sleeper = Mock()

    result = SecFinancialIngestionService(
        client, request_delay_seconds=0, sleeper=sleeper
    ).ingest_company_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "bronze")

    assert result.submissions.record_count == 1
    assert result.company_facts.record_count == 1
    assert result.submissions.parquet_path.is_file()
    assert result.company_facts.parquet_path.is_file()
    reused = SecFinancialIngestionService(
        client, request_delay_seconds=0, sleeper=sleeper
    ).ingest_company_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "bronze")
    assert reused.submissions.record_count == 1
    assert reused.company_facts.record_count == 1
    client.fetch_submissions.assert_not_called()
    client.fetch_company_facts.assert_not_called()
    sleeper.assert_not_called()


def test_prevalidation_and_combined_collision_make_no_requests_or_sleeps(
    tmp_path: Path,
) -> None:
    client = Mock(spec=SecEdgarClient)
    sleeper = Mock()
    service = SecFinancialIngestionService(client, sleeper=sleeper)

    with pytest.raises(ValueError):
        service.ingest_company_to_bronze(
            "invalid",
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )
    with pytest.raises(ValueError):
        service.ingest_company_to_bronze(
            320193,
            run_at=datetime(2026, 8, 30, 12, 0),
            bronze_root=tmp_path / "bronze",
        )

    collision = _location(tmp_path, SEC_COMPANY_FACTS_BRONZE_DATASET)
    write_raw_json(collision, {"existing": "artifact"})
    with pytest.raises(BronzeRecoveryError):
        service.ingest_company_to_bronze(
            320193,
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    client.fetch_submissions.assert_not_called()
    client.fetch_company_facts.assert_not_called()
    sleeper.assert_not_called()
    assert read_raw_json(collision) == {"existing": "artifact"}


@pytest.mark.parametrize(
    "payload, expected_error",
    [
        pytest.param(None, SecEdgarError, id="provider"),
        pytest.param({}, SecSubmissionsValidationError, id="parser"),
    ],
)
def test_submissions_failures_leave_no_bronze_artifacts(
    tmp_path: Path,
    payload: object,
    expected_error: type[Exception],
) -> None:
    client = Mock(spec=SecEdgarClient)
    if payload is None:
        client.fetch_submissions.side_effect = SecEdgarError("provider failed")
    else:
        client.fetch_submissions.return_value = payload
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    with pytest.raises(expected_error):
        service.ingest_company_to_bronze(
            320193,
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    assert not (tmp_path / "bronze").exists()
    client.fetch_company_facts.assert_not_called()


def test_later_company_facts_failure_retains_submissions_artifacts(
    tmp_path: Path,
) -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = _submissions_payload(_filing_row())
    client.fetch_company_facts.side_effect = SecEdgarError("facts failed")
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    with pytest.raises(SecEdgarError, match="facts failed"):
        service.ingest_company_to_bronze(
            320193,
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    submissions = _location(tmp_path, SEC_SUBMISSIONS_BRONZE_DATASET)
    facts = _location(tmp_path, SEC_COMPANY_FACTS_BRONZE_DATASET)
    assert read_raw_json(submissions) == _submissions_payload(_filing_row())
    assert read_parquet(submissions).num_rows == 1
    assert not facts.directory.exists()


def test_company_facts_parquet_failure_retains_both_raw_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submissions_payload = _submissions_payload(_filing_row())
    facts_payload = _company_facts_payload(_occurrence())
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = submissions_payload
    client.fetch_company_facts.return_value = facts_payload
    real_write_parquet = sec_ingestion.write_parquet
    calls = 0

    def fail_second_parquet(location: BronzeRunLocation, table: pa.Table) -> Path:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise BronzeParquetWriteError("facts parquet failed")
        return real_write_parquet(location, table)

    monkeypatch.setattr(sec_ingestion, "write_parquet", fail_second_parquet)

    with pytest.raises(BronzeParquetWriteError, match="facts parquet failed"):
        SecFinancialIngestionService(
            client,
            request_delay_seconds=0,
        ).ingest_company_to_bronze(
            320193,
            run_at=_RUN_AT,
            bronze_root=tmp_path / "bronze",
        )

    submissions = _location(tmp_path, SEC_SUBMISSIONS_BRONZE_DATASET)
    facts = _location(tmp_path, SEC_COMPANY_FACTS_BRONZE_DATASET)
    assert read_raw_json(submissions) == submissions_payload
    assert read_parquet(submissions).num_rows == 1
    assert read_raw_json(facts) == facts_payload



def test_nested_converter_records_raise_sec_bronze_validation_error() -> None:
    malformed_submissions = SecSubmissions(cik=_CIK, company_name="Apple Inc.", filings=(object(),))  # type: ignore[arg-type]
    malformed_facts = SecCompanyFacts(cik=_CIK, entity_name="Apple Inc.", facts=(object(),))  # type: ignore[arg-type]
    with pytest.raises(SecBronzeValidationError):
        sec_submissions_to_table(malformed_submissions)
    with pytest.raises(SecBronzeValidationError):
        sec_company_facts_to_table(malformed_facts)


def test_company_facts_schema_locks_full_type_sequence() -> None:
    assert list(SEC_COMPANY_FACTS_SCHEMA.types) == [
        pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.decimal256(76, 30), pa.date32(), pa.date32(), pa.string(), pa.int32(), pa.string(), pa.string(), pa.date32(), pa.string(),
    ]


def test_endpoint_collision_prevents_request_and_sleep(tmp_path: Path) -> None:
    location = _location(tmp_path, SEC_SUBMISSIONS_BRONZE_DATASET)
    write_raw_json(location, {"existing": "artifact"})
    client = Mock(spec=SecEdgarClient)
    sleeper = Mock()
    with pytest.raises(BronzeRecoveryError):
        SecFinancialIngestionService(client, sleeper=sleeper).ingest_submissions_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "bronze")
    client.fetch_submissions.assert_not_called()
    sleeper.assert_not_called()
    assert read_raw_json(location) == {"existing": "artifact"}


def test_submissions_failure_ordering(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _submissions_payload(_filing_row())
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = payload
    service = SecFinancialIngestionService(client, request_delay_seconds=0)
    monkeypatch.setattr(sec_ingestion, "sec_submissions_to_table", Mock(side_effect=SecBronzeValidationError("arrow failed")))
    with pytest.raises(SecBronzeValidationError):
        service.ingest_company_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "arrow")
    assert not (tmp_path / "arrow").exists()
    client.fetch_company_facts.assert_not_called()
    parquet_writer = Mock()
    monkeypatch.setattr(sec_ingestion, "sec_submissions_to_table", sec_submissions_to_table)
    monkeypatch.setattr(sec_ingestion, "write_raw_json", Mock(side_effect=BronzeRawJsonValidationError("raw failed")))
    monkeypatch.setattr(sec_ingestion, "write_parquet", parquet_writer)
    with pytest.raises(BronzeRawJsonValidationError):
        service.ingest_company_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "raw")
    parquet_writer.assert_not_called()
    monkeypatch.setattr(sec_ingestion, "write_raw_json", write_raw_json)
    monkeypatch.setattr(sec_ingestion, "write_parquet", Mock(side_effect=BronzeParquetWriteError("parquet failed")))
    with pytest.raises(BronzeParquetWriteError):
        service.ingest_company_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "parquet")
    location = BronzeRunLocation.from_run(root=tmp_path / "parquet", source=SEC_BRONZE_SOURCE, dataset=SEC_SUBMISSIONS_BRONZE_DATASET, ingested_at=_RUN_AT, entity="0000320193")
    assert read_raw_json(location) == payload
    client.fetch_company_facts.assert_not_called()


def test_later_company_facts_failure_ordering(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    submissions_payload = _submissions_payload(_filing_row())
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = submissions_payload
    client.fetch_company_facts.return_value = {}
    service = SecFinancialIngestionService(client, request_delay_seconds=0)
    with pytest.raises(SecCompanyFactsValidationError):
        service.ingest_company_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "parser")
    submissions = BronzeRunLocation.from_run(root=tmp_path / "parser", source=SEC_BRONZE_SOURCE, dataset=SEC_SUBMISSIONS_BRONZE_DATASET, ingested_at=_RUN_AT, entity="0000320193")
    assert read_parquet(submissions).num_rows == 1
    client.fetch_company_facts.return_value = _company_facts_payload(_occurrence())
    monkeypatch.setattr(sec_ingestion, "sec_company_facts_to_table", Mock(side_effect=SecBronzeValidationError("facts arrow")))
    with pytest.raises(SecBronzeValidationError):
        service.ingest_company_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "arrow")
    assert not (tmp_path / "arrow" / SEC_BRONZE_SOURCE / SEC_COMPANY_FACTS_BRONZE_DATASET).exists()
    real_raw = sec_ingestion.write_raw_json
    parquet_writer = Mock(side_effect=sec_ingestion.write_parquet)
    monkeypatch.setattr(sec_ingestion, "sec_company_facts_to_table", sec_company_facts_to_table)
    def raw_writer(location: BronzeRunLocation, payload: dict) -> Path:
        if location.metadata.dataset == SEC_COMPANY_FACTS_BRONZE_DATASET:
            raise BronzeRawJsonValidationError("facts raw")
        return real_raw(location, payload)
    monkeypatch.setattr(sec_ingestion, "write_raw_json", raw_writer)
    monkeypatch.setattr(sec_ingestion, "write_parquet", parquet_writer)
    with pytest.raises(BronzeRawJsonValidationError):
        service.ingest_company_to_bronze(320193, run_at=_RUN_AT, bronze_root=tmp_path / "raw")
    assert parquet_writer.call_count == 1

