import csv
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from finstream.sec.companies import INITIAL_SEC_COMPANIES, SecCompanyConfig
from finstream.sec.companies import (
    HISTORICAL_SEC_REGISTRANTS,
    INITIAL_SEC_COMPANIES,
    HistoricalSecRegistrantConfig,
    SecCompanyConfig,
)


_EXPECTED_COMPANIES = (
    ("AAPL", "0000320193"),
    ("MSFT", "0000789019"),
    ("NVDA", "0001045810"),
    ("AMZN", "0001018724"),
    ("XOM", "0002115436"),
    ("WMT", "0000104169"),
)


def test_initial_sec_companies_have_exact_order_and_mappings() -> None:
    assert len(INITIAL_SEC_COMPANIES) == 6
    assert tuple(
        (company.ticker, company.cik) for company in INITIAL_SEC_COMPANIES
    ) == _EXPECTED_COMPANIES


def test_configured_ciks_are_canonical_nonzero_strings() -> None:
    for company in INITIAL_SEC_COMPANIES:
        assert isinstance(company.cik, str)
        assert len(company.cik) == 10
        assert company.cik.isdigit()
        assert int(company.cik) > 0


def test_configured_tickers_and_ciks_are_unique() -> None:
    tickers = [company.ticker for company in INITIAL_SEC_COMPANIES]
    ciks = [company.cik for company in INITIAL_SEC_COMPANIES]

    assert len(tickers) == len(set(tickers))
    assert len(ciks) == len(set(ciks))


def test_sec_company_config_is_immutable() -> None:
    company = SecCompanyConfig(ticker="AAPL", cik="0000320193")

    with pytest.raises(FrozenInstanceError):
        company.ticker = "MSFT"  # type: ignore[misc]


def test_current_company_identity_seed_matches_initial_sec_companies() -> None:
    seed_path = (
        Path(__file__).resolve().parents[2]
        / "dbt"
        / "seeds"
        / "company_identity_mapping.csv"
    )
    assert seed_path.is_file(), f"Seed file not found at {seed_path}"

    with open(seed_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        current_seed_mappings = {
            row["current_ticker"]: row["source_cik"]
            for row in reader
            if row["is_current_registrant"].strip().lower() == "true"
        }

    expected_mappings = {
        company.ticker: company.cik for company in INITIAL_SEC_COMPANIES
    }

    assert current_seed_mappings == expected_mappings


def test_historical_sec_registrants_have_exact_order_and_mappings() -> None:
    assert len(HISTORICAL_SEC_REGISTRANTS) == 1
    registrant = HISTORICAL_SEC_REGISTRANTS[0]
    assert registrant.company_key == "XOM"
    assert registrant.cik == "0000034088"
    assert registrant.task_key == "sec_xom_predecessor"


def test_historical_configured_ciks_are_canonical_nonzero_strings() -> None:
    for registrant in HISTORICAL_SEC_REGISTRANTS:
        assert isinstance(registrant.cik, str)
        assert len(registrant.cik) == 10
        assert registrant.cik.isdigit()
        assert int(registrant.cik) > 0


def test_historical_ciks_do_not_overlap_with_initial_sec_companies() -> None:
    current_ciks = {company.cik for company in INITIAL_SEC_COMPANIES}
    historical_ciks = {registrant.cik for registrant in HISTORICAL_SEC_REGISTRANTS}
    assert not (current_ciks & historical_ciks)


def test_historical_sec_registrant_config_is_immutable() -> None:
    registrant = HistoricalSecRegistrantConfig(
        company_key="XOM",
        cik="0000034088",
        task_key="sec_xom_predecessor",
    )

    with pytest.raises(FrozenInstanceError):
        registrant.cik = "0002115436"  # type: ignore[misc]


def test_predecessor_company_identity_seed_matches_historical_sec_registrants() -> None:
    seed_path = (
        Path(__file__).resolve().parents[2]
        / "dbt"
        / "seeds"
        / "company_identity_mapping.csv"
    )
    assert seed_path.is_file(), f"Seed file not found at {seed_path}"

    with open(seed_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        predecessor_rows = [
            (row["company_key"], row["source_cik"], row["registrant_role"])
            for row in reader
            if row["is_current_registrant"].strip().lower() == "false"
        ]

    expected_predecessors = [
        (registrant.company_key, registrant.cik, "predecessor")
        for registrant in HISTORICAL_SEC_REGISTRANTS
    ]

    assert predecessor_rows == expected_predecessors
