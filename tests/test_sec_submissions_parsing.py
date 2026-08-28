from datetime import date, datetime, timezone
from typing import Any

import pytest

from finflow.sec.models import SecFilingMetadata, SecSubmissions
from finflow.sec.parsing import SecSubmissionsValidationError, parse_submissions


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


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "accessionNumber": "0000001234-26-123456",
        "filingDate": "2026-08-01",
        "reportDate": "2026-06-30",
        "acceptanceDateTime": "2026-08-01T10:01:02.000Z",
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


def _recent(*rows: dict[str, object]) -> dict[str, list[object]]:
    return {field_name: [row[field_name] for row in rows] for field_name in _RECENT_FIELDS}


def _payload(
    *rows: dict[str, object],
    cik: object = "320193",
    name: object = "  Apple Inc.  ",
) -> dict[str, Any]:
    return {
        "cik": cik,
        "name": name,
        "filings": {"recent": _recent(*rows), "files": []},
    }


def test_parses_valid_recent_filing_metadata() -> None:
    result = parse_submissions(_payload(_row()), expected_cik=320193)

    assert result == SecSubmissions(
        cik="0000320193",
        company_name="Apple Inc.",
        filings=(
            SecFilingMetadata(
                cik="0000320193",
                accession_number="0000001234-26-123456",
                filing_date=date(2026, 8, 1),
                report_date=date(2026, 6, 30),
                acceptance_datetime=datetime(
                    2026, 8, 1, 10, 1, 2, tzinfo=timezone.utc
                ),
                form="10-Q",
                act="34",
                file_number="001-36743",
                film_number="261234567",
                items=None,
                size=123456,
                is_xbrl=True,
                is_inline_xbrl=True,
                primary_document="form10-q.htm",
                primary_doc_description="FORM 10-Q",
            ),
        ),
    )


def test_preserves_column_alignment_and_provider_order() -> None:
    first = _row()
    second = _row(
        accessionNumber="0000005678-26-654321",
        filingDate="2026-07-15",
        reportDate="2025-12-31",
        form="8-K",
        primaryDocument="form8-k.htm",
    )

    result = parse_submissions(_payload(first, second), expected_cik="0000320193")

    assert [filing.accession_number for filing in result.filings] == [
        "0000001234-26-123456",
        "0000005678-26-654321",
    ]
    assert [filing.form for filing in result.filings] == ["10-Q", "8-K"]
    assert [filing.filing_date for filing in result.filings] == [
        date(2026, 8, 1),
        date(2026, 7, 15),
    ]
    assert result.filings[1].primary_document == "form8-k.htm"


def test_accepts_empty_aligned_recent_arrays() -> None:
    result = parse_submissions(_payload(), expected_cik=320193)

    assert result.filings == ()


def test_rejects_payload_cik_mismatch() -> None:
    with pytest.raises(SecSubmissionsValidationError, match="does not match"):
        parse_submissions(_payload(_row(), cik="789019"), expected_cik=320193)


@pytest.mark.parametrize("payload", [None, [], "invalid"])
def test_rejects_non_object_payload(payload: object) -> None:
    with pytest.raises(SecSubmissionsValidationError, match="payload must be an object"):
        parse_submissions(payload, expected_cik=320193)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", [None, "", "   ", 123])
def test_rejects_missing_or_invalid_company_name(name: object) -> None:
    with pytest.raises(SecSubmissionsValidationError, match="name"):
        parse_submissions(_payload(_row(), name=name), expected_cik=320193)


@pytest.mark.parametrize("filings", [None, [], "invalid"])
def test_rejects_missing_or_invalid_filings(filings: object) -> None:
    payload = _payload(_row())
    payload["filings"] = filings

    with pytest.raises(SecSubmissionsValidationError, match="filings must be an object"):
        parse_submissions(payload, expected_cik=320193)


@pytest.mark.parametrize("recent", [None, [], "invalid"])
def test_rejects_missing_or_invalid_recent(recent: object) -> None:
    payload = _payload(_row())
    payload["filings"]["recent"] = recent

    with pytest.raises(SecSubmissionsValidationError, match="filings.recent"):
        parse_submissions(payload, expected_cik=320193)


@pytest.mark.parametrize("field_name", _RECENT_FIELDS)
def test_rejects_missing_modeled_recent_field(field_name: str) -> None:
    payload = _payload(_row())
    del payload["filings"]["recent"][field_name]

    with pytest.raises(SecSubmissionsValidationError, match=field_name):
        parse_submissions(payload, expected_cik=320193)


@pytest.mark.parametrize("field_name", _RECENT_FIELDS)
def test_rejects_non_list_modeled_recent_field(field_name: str) -> None:
    payload = _payload(_row())
    payload["filings"]["recent"][field_name] = "invalid"

    with pytest.raises(SecSubmissionsValidationError, match=field_name):
        parse_submissions(payload, expected_cik=320193)


def test_rejects_unequal_recent_array_lengths() -> None:
    payload = _payload(_row())
    payload["filings"]["recent"]["form"].append("8-K")

    with pytest.raises(SecSubmissionsValidationError, match="equal lengths"):
        parse_submissions(payload, expected_cik=320193)


@pytest.mark.parametrize("accession_number", ["", "   ", "320193-26-123456"])
def test_rejects_invalid_accession_number(accession_number: str) -> None:
    with pytest.raises(SecSubmissionsValidationError, match="accessionNumber"):
        parse_submissions(
            _payload(_row(accessionNumber=accession_number)),
            expected_cik=320193,
        )


def test_rejects_duplicate_accession_numbers() -> None:
    with pytest.raises(SecSubmissionsValidationError, match="duplicate"):
        parse_submissions(_payload(_row(), _row()), expected_cik=320193)


def test_maps_blank_optional_fields_and_zero_flags() -> None:
    row = _row(
        reportDate="",
        acceptanceDateTime="",
        act=" ",
        fileNumber="",
        filmNumber="  ",
        items="",
        primaryDocument=" ",
        primaryDocDescription="",
        size=0,
        isXBRL=0,
        isInlineXBRL=0,
    )

    filing = parse_submissions(_payload(row), expected_cik=320193).filings[0]

    assert filing.report_date is None
    assert filing.acceptance_datetime is None
    assert filing.act is None
    assert filing.file_number is None
    assert filing.film_number is None
    assert filing.items is None
    assert filing.primary_document is None
    assert filing.primary_doc_description is None
    assert filing.size == 0
    assert filing.is_xbrl is False
    assert filing.is_inline_xbrl is False


@pytest.mark.parametrize("filing_date", [None, "2026-8-01", "2026-02-30"])
def test_rejects_invalid_filing_date(filing_date: object) -> None:
    with pytest.raises(SecSubmissionsValidationError, match="filingDate"):
        parse_submissions(
            _payload(_row(filingDate=filing_date)),
            expected_cik=320193,
        )


@pytest.mark.parametrize("report_date", [None, "2026-6-30", "2026-02-30"])
def test_rejects_invalid_nonblank_report_date(report_date: object) -> None:
    with pytest.raises(SecSubmissionsValidationError, match="reportDate"):
        parse_submissions(
            _payload(_row(reportDate=report_date)),
            expected_cik=320193,
        )


@pytest.mark.parametrize("acceptance_datetime", [None, "invalid"])
def test_rejects_invalid_acceptance_datetime(acceptance_datetime: object) -> None:
    with pytest.raises(SecSubmissionsValidationError, match="acceptanceDateTime"):
        parse_submissions(
            _payload(_row(acceptanceDateTime=acceptance_datetime)),
            expected_cik=320193,
        )


def test_rejects_timezone_naive_acceptance_datetime() -> None:
    with pytest.raises(SecSubmissionsValidationError, match="timezone"):
        parse_submissions(
            _payload(_row(acceptanceDateTime="2026-08-01T10:01:02.000")),
            expected_cik=320193,
        )


@pytest.mark.parametrize("form", [None, "", "   "])
def test_rejects_invalid_form(form: object) -> None:
    with pytest.raises(SecSubmissionsValidationError, match="form"):
        parse_submissions(_payload(_row(form=form)), expected_cik=320193)


@pytest.mark.parametrize(
    "field_name",
    ["act", "fileNumber", "filmNumber", "items", "primaryDocument", "primaryDocDescription"],
)
def test_rejects_non_string_optional_source_field(field_name: str) -> None:
    with pytest.raises(SecSubmissionsValidationError, match=field_name):
        parse_submissions(
            _payload(_row(**{field_name: None})),
            expected_cik=320193,
        )


@pytest.mark.parametrize("size", [-1, True, "1", 1.5])
def test_rejects_invalid_size(size: object) -> None:
    with pytest.raises(SecSubmissionsValidationError, match="size"):
        parse_submissions(_payload(_row(size=size)), expected_cik=320193)


@pytest.mark.parametrize("field_name", ["isXBRL", "isInlineXBRL"])
@pytest.mark.parametrize("flag", [-1, 2, True, "1"])
def test_rejects_invalid_xbrl_flag(field_name: str, flag: object) -> None:
    with pytest.raises(SecSubmissionsValidationError, match=field_name):
        parse_submissions(
            _payload(_row(**{field_name: flag})),
            expected_cik=320193,
        )


def test_ignores_extra_recent_fields() -> None:
    payload = _payload(_row())
    payload["filings"]["recent"]["extraProviderField"] = ["source value"]

    result = parse_submissions(payload, expected_cik=320193)

    assert len(result.filings) == 1
