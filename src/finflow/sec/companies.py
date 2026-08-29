"""Configured SEC company identities for FinFlow V1."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SecCompanyConfig:
    """Current ticker and canonical SEC CIK for one configured company."""

    ticker: str
    cik: str


INITIAL_SEC_COMPANIES: tuple[SecCompanyConfig, ...] = (
    SecCompanyConfig(ticker="AAPL", cik="0000320193"),
    SecCompanyConfig(ticker="MSFT", cik="0000789019"),
    SecCompanyConfig(ticker="NVDA", cik="0001045810"),
    SecCompanyConfig(ticker="AMZN", cik="0001018724"),
    SecCompanyConfig(ticker="XOM", cik="0002115436"),
    SecCompanyConfig(ticker="WMT", cik="0000104169"),
)
