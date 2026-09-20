from dataclasses import FrozenInstanceError

import pytest

from finstream.sec.companies import INITIAL_SEC_COMPANIES, SecCompanyConfig


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
