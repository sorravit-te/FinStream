"""Application service for retrieving and parsing SEC financial source data."""

import math
import time
from collections.abc import Callable, Iterable
from typing import Any

from finstream.sec.edgar import SecEdgarClient, _normalize_cik
from finstream.sec.models import SecCompanyFacts, SecCompanySourceData, SecSubmissions
from finstream.sec.parsing import parse_company_facts, parse_submissions


class SecFinancialIngestionService:
    """Coordinate paced SEC retrieval and source-aligned parsing."""

    def __init__(
        self,
        client: SecEdgarClient,
        *,
        request_delay_seconds: float = 0.125,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if (
            isinstance(request_delay_seconds, bool)
            or not isinstance(request_delay_seconds, (int, float))
            or request_delay_seconds < 0
            or (
                isinstance(request_delay_seconds, float)
                and not math.isfinite(request_delay_seconds)
            )
        ):
            raise ValueError("Request delay must be a non-negative finite number")

        self._client = client
        self._request_delay_seconds = float(request_delay_seconds)
        self._sleeper = sleeper
        self._has_made_request = False

    def ingest_submissions(self, cik: str | int) -> SecSubmissions:
        """Retrieve and parse recent filing metadata for one CIK."""
        return self._ingest_submissions(_normalize_cik(cik))

    def ingest_company_facts(self, cik: str | int) -> SecCompanyFacts:
        """Retrieve and parse Company Facts for one CIK."""
        return self._ingest_company_facts(_normalize_cik(cik))

    def ingest_company(self, cik: str | int) -> SecCompanySourceData:
        """Retrieve both SEC source payloads for one CIK in request order."""
        return self._ingest_company(_normalize_cik(cik))

    def ingest_companies(
        self,
        ciks: Iterable[str | int],
    ) -> dict[str, SecCompanySourceData]:
        """Validate, then ingest companies sequentially in input order."""
        normalized_ciks: list[str] = []
        seen_ciks: set[str] = set()
        for cik in ciks:
            normalized_cik = _normalize_cik(cik)
            if normalized_cik in seen_ciks:
                raise ValueError(f"Duplicate CIK: {normalized_cik}")
            normalized_ciks.append(normalized_cik)
            seen_ciks.add(normalized_cik)

        results: dict[str, SecCompanySourceData] = {}
        for normalized_cik in normalized_ciks:
            results[normalized_cik] = self._ingest_company(normalized_cik)
        return results

    def _request(
        self,
        request: Callable[[str], dict[str, Any]],
        normalized_cik: str,
    ) -> dict[str, Any]:
        if self._has_made_request and self._request_delay_seconds > 0:
            self._sleeper(self._request_delay_seconds)
        self._has_made_request = True
        return request(normalized_cik)

    def _ingest_submissions(self, normalized_cik: str) -> SecSubmissions:
        payload = self._request(self._client.fetch_submissions, normalized_cik)
        return parse_submissions(payload, expected_cik=normalized_cik)

    def _ingest_company_facts(self, normalized_cik: str) -> SecCompanyFacts:
        payload = self._request(self._client.fetch_company_facts, normalized_cik)
        return parse_company_facts(payload, expected_cik=normalized_cik)

    def _ingest_company(self, normalized_cik: str) -> SecCompanySourceData:
        submissions = self._ingest_submissions(normalized_cik)
        company_facts = self._ingest_company_facts(normalized_cik)
        return SecCompanySourceData(
            cik=normalized_cik,
            submissions=submissions,
            company_facts=company_facts,
        )
