from unittest.mock import Mock, call

import pytest

from finflow.sec.edgar import SecEdgarClient, SecEdgarError
from finflow.sec.ingestion import SecFinancialIngestionService
from finflow.sec.models import SecCompanySourceData
from finflow.sec.parsing import (
    SecCompanyFactsValidationError,
    SecSubmissionsValidationError,
)


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


def _submissions_payload(
    cik: str = "0000320193",
    *,
    name: str = "Apple Inc.",
) -> dict:
    return {
        "cik": cik,
        "name": name,
        "filings": {
            "recent": {field_name: [] for field_name in _RECENT_FIELDS},
            "files": [],
        },
    }


def _company_facts_payload(
    cik: str = "0000320193",
    *,
    entity_name: str = "Apple Inc.",
) -> dict:
    return {"cik": cik, "entityName": entity_name, "facts": {}}


def _client_for_empty_payloads() -> Mock:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.side_effect = lambda cik: _submissions_payload(cik)
    client.fetch_company_facts.side_effect = lambda cik: _company_facts_payload(cik)
    return client


def test_ingests_submissions_with_canonical_cik() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = _submissions_payload()
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    result = service.ingest_submissions(320193)

    client.fetch_submissions.assert_called_once_with("0000320193")
    assert result.cik == "0000320193"
    assert result.company_name == "Apple Inc."


def test_ingests_company_facts_with_canonical_cik() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_company_facts.return_value = _company_facts_payload()
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    result = service.ingest_company_facts("320193")

    client.fetch_company_facts.assert_called_once_with("0000320193")
    assert result.cik == "0000320193"
    assert result.entity_name == "Apple Inc."


def test_ingests_complete_company_in_exact_request_order() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = _submissions_payload(name="Apple")
    client.fetch_company_facts.return_value = _company_facts_payload(
        entity_name="Apple Inc."
    )
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    result = service.ingest_company(320193)

    assert client.method_calls == [
        call.fetch_submissions("0000320193"),
        call.fetch_company_facts("0000320193"),
    ]
    assert isinstance(result, SecCompanySourceData)
    assert result.cik == "0000320193"
    assert result.submissions.company_name == "Apple"
    assert result.company_facts.entity_name == "Apple Inc."


def test_default_pacing_occurs_between_complete_company_requests() -> None:
    events: list[object] = []
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.side_effect = lambda cik: (
        events.append(("submissions", cik)) or _submissions_payload(cik)
    )
    client.fetch_company_facts.side_effect = lambda cik: (
        events.append(("company_facts", cik)) or _company_facts_payload(cik)
    )
    sleeper = Mock(side_effect=lambda delay: events.append(("sleep", delay)))
    service = SecFinancialIngestionService(client, sleeper=sleeper)

    service.ingest_company(320193)

    assert events == [
        ("submissions", "0000320193"),
        ("sleep", 0.125),
        ("company_facts", "0000320193"),
    ]
    sleeper.assert_called_once_with(0.125)


def test_uses_custom_request_delay() -> None:
    client = _client_for_empty_payloads()
    sleeper = Mock()
    service = SecFinancialIngestionService(
        client,
        request_delay_seconds=0.25,
        sleeper=sleeper,
    )

    service.ingest_company(320193)

    sleeper.assert_called_once_with(0.25)


def test_zero_delay_disables_sleep_without_disabling_requests() -> None:
    client = _client_for_empty_payloads()
    sleeper = Mock()
    service = SecFinancialIngestionService(
        client,
        request_delay_seconds=0,
        sleeper=sleeper,
    )

    service.ingest_company(320193)

    assert client.method_calls == [
        call.fetch_submissions("0000320193"),
        call.fetch_company_facts("0000320193"),
    ]
    sleeper.assert_not_called()


@pytest.mark.parametrize("request_delay", [-0.1, True, "0.125", None])
def test_rejects_invalid_request_delay(request_delay: object) -> None:
    with pytest.raises(ValueError, match="Request delay"):
        SecFinancialIngestionService(
            Mock(spec=SecEdgarClient),
            request_delay_seconds=request_delay,  # type: ignore[arg-type]
        )


def test_submissions_provider_failure_stops_company_ingestion() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.side_effect = SecEdgarError("provider failed")
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    with pytest.raises(SecEdgarError, match="provider failed"):
        service.ingest_company(320193)

    client.fetch_company_facts.assert_not_called()


def test_submissions_validation_failure_stops_company_ingestion() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = {}
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    with pytest.raises(SecSubmissionsValidationError):
        service.ingest_company(320193)

    client.fetch_company_facts.assert_not_called()


def test_company_facts_provider_failure_propagates_without_result() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = _submissions_payload()
    client.fetch_company_facts.side_effect = SecEdgarError("provider failed")
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    with pytest.raises(SecEdgarError, match="provider failed"):
        service.ingest_company(320193)

    assert client.method_calls == [
        call.fetch_submissions("0000320193"),
        call.fetch_company_facts("0000320193"),
    ]


def test_company_facts_validation_failure_propagates() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = _submissions_payload()
    client.fetch_company_facts.return_value = {}
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    with pytest.raises(SecCompanyFactsValidationError):
        service.ingest_company(320193)


def test_valid_empty_submissions_remains_empty() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = _submissions_payload()
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    result = service.ingest_submissions(320193)

    assert result.filings == ()


def test_valid_empty_company_facts_remains_empty() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_company_facts.return_value = _company_facts_payload()
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    result = service.ingest_company_facts(320193)

    assert result.facts == ()


def test_ingests_multiple_companies_with_canonical_ordered_keys() -> None:
    client = _client_for_empty_payloads()
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    result = service.ingest_companies([320193, "789019"])

    assert list(result) == ["0000320193", "0000789019"]
    assert [company.cik for company in result.values()] == [
        "0000320193",
        "0000789019",
    ]


def test_rejects_duplicate_normalized_ciks_before_provider_calls() -> None:
    client = Mock(spec=SecEdgarClient)
    sleeper = Mock()
    service = SecFinancialIngestionService(client, sleeper=sleeper)

    with pytest.raises(ValueError, match="Duplicate CIK"):
        service.ingest_companies([320193, "0000320193"])

    client.fetch_submissions.assert_not_called()
    client.fetch_company_facts.assert_not_called()
    sleeper.assert_not_called()


def test_rejects_any_invalid_cik_before_provider_calls() -> None:
    client = Mock(spec=SecEdgarClient)
    sleeper = Mock()
    service = SecFinancialIngestionService(client, sleeper=sleeper)

    with pytest.raises(ValueError):
        service.ingest_companies([320193, "invalid", 789019])

    client.fetch_submissions.assert_not_called()
    client.fetch_company_facts.assert_not_called()
    sleeper.assert_not_called()


def test_empty_multi_company_input_does_nothing() -> None:
    client = Mock(spec=SecEdgarClient)
    sleeper = Mock()
    service = SecFinancialIngestionService(client, sleeper=sleeper)

    assert service.ingest_companies([]) == {}
    client.fetch_submissions.assert_not_called()
    client.fetch_company_facts.assert_not_called()
    sleeper.assert_not_called()


def test_two_companies_preserve_sequential_request_order_and_pacing() -> None:
    events: list[object] = []
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.side_effect = lambda cik: (
        events.append(("submissions", cik)) or _submissions_payload(cik)
    )
    client.fetch_company_facts.side_effect = lambda cik: (
        events.append(("company_facts", cik)) or _company_facts_payload(cik)
    )
    sleeper = Mock(side_effect=lambda delay: events.append(("sleep", delay)))
    service = SecFinancialIngestionService(client, sleeper=sleeper)

    service.ingest_companies([320193, 789019])

    assert events == [
        ("submissions", "0000320193"),
        ("sleep", 0.125),
        ("company_facts", "0000320193"),
        ("sleep", 0.125),
        ("submissions", "0000789019"),
        ("sleep", 0.125),
        ("company_facts", "0000789019"),
    ]
    assert sleeper.call_args_list == [call(0.125), call(0.125), call(0.125)]


def test_multi_company_failure_stops_later_requests() -> None:
    client = Mock(spec=SecEdgarClient)

    def fetch_submissions(cik: str) -> dict:
        if cik == "0000789019":
            raise SecEdgarError("provider failed")
        return _submissions_payload(cik)

    client.fetch_submissions.side_effect = fetch_submissions
    client.fetch_company_facts.side_effect = lambda cik: _company_facts_payload(cik)
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    with pytest.raises(SecEdgarError, match="provider failed"):
        service.ingest_companies([320193, 789019, 123456])

    assert client.method_calls == [
        call.fetch_submissions("0000320193"),
        call.fetch_company_facts("0000320193"),
        call.fetch_submissions("0000789019"),
    ]


def test_parser_cik_mismatch_propagates() -> None:
    client = Mock(spec=SecEdgarClient)
    client.fetch_submissions.return_value = _submissions_payload("0000789019")
    service = SecFinancialIngestionService(client, request_delay_seconds=0)

    with pytest.raises(SecSubmissionsValidationError, match="does not match"):
        service.ingest_submissions(320193)
