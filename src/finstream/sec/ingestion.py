"""Application service for retrieving and parsing SEC financial source data."""

import math
import time
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from finstream.bronze.json_storage import write_raw_json
from finstream.bronze.models import BronzeRunLocation
from finstream.bronze.parquet_storage import write_parquet
from finstream.bronze.paths import DEFAULT_BRONZE_ROOT
from finstream.bronze.recovery import recover_or_verify_bronze_artifacts
from finstream.sec.bronze import (
    SEC_BRONZE_SOURCE,
    SEC_COMPANY_FACTS_BRONZE_DATASET,
    SEC_SUBMISSIONS_BRONZE_DATASET,
    SecBronzeDatasetResult,
    SecCompanyBronzeResult,
    sec_company_facts_to_table,
    sec_submissions_to_table,
)
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

    def ingest_submissions_to_bronze(
        self,
        cik: str | int,
        *,
        run_at: datetime,
        bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
    ) -> SecBronzeDatasetResult:
        """Retrieve and persist one SEC submissions Bronze dataset."""
        normalized_cik = _normalize_cik(cik)
        location = self._bronze_location(
            dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        recovered = self._recover_submissions_bronze(normalized_cik, location)
        if recovered is not None:
            return recovered
        return self._ingest_submissions_to_bronze(normalized_cik, location)

    def ingest_company_facts_to_bronze(
        self,
        cik: str | int,
        *,
        run_at: datetime,
        bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
    ) -> SecBronzeDatasetResult:
        """Retrieve and persist one SEC Company Facts Bronze dataset."""
        normalized_cik = _normalize_cik(cik)
        location = self._bronze_location(
            dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        recovered = self._recover_company_facts_bronze(normalized_cik, location)
        if recovered is not None:
            return recovered
        return self._ingest_company_facts_to_bronze(normalized_cik, location)

    def ingest_company_to_bronze(
        self,
        cik: str | int,
        *,
        run_at: datetime,
        bronze_root: str | Path = DEFAULT_BRONZE_ROOT,
    ) -> SecCompanyBronzeResult:
        """Retrieve and persist both SEC Bronze datasets in request order."""
        normalized_cik = _normalize_cik(cik)
        submissions_location = self._bronze_location(
            dataset=SEC_SUBMISSIONS_BRONZE_DATASET,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        company_facts_location = self._bronze_location(
            dataset=SEC_COMPANY_FACTS_BRONZE_DATASET,
            run_at=run_at,
            bronze_root=bronze_root,
        )
        submissions = self._recover_submissions_bronze(
            normalized_cik,
            submissions_location,
        )
        company_facts = self._recover_company_facts_bronze(
            normalized_cik,
            company_facts_location,
        )
        if submissions is None:
            submissions = self._ingest_submissions_to_bronze(
                normalized_cik,
                submissions_location,
            )
        if company_facts is None:
            company_facts = self._ingest_company_facts_to_bronze(
                normalized_cik,
                company_facts_location,
            )
        return SecCompanyBronzeResult(
            cik=normalized_cik,
            submissions=submissions,
            company_facts=company_facts,
        )

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

    @staticmethod
    def _bronze_location(
        *,
        dataset: str,
        run_at: datetime,
        bronze_root: str | Path,
    ) -> BronzeRunLocation:
        return BronzeRunLocation.from_run(
            root=bronze_root,
            source=SEC_BRONZE_SOURCE,
            dataset=dataset,
            ingested_at=run_at,
        )

    @staticmethod
    def _recovered_dataset_result(
        cik: str,
        location: BronzeRunLocation,
        record_count: int,
    ) -> SecBronzeDatasetResult:
        return SecBronzeDatasetResult(
            cik=cik,
            location=location,
            raw_json_path=location.directory / "payload.json",
            parquet_path=location.directory / "data.parquet",
            record_count=record_count,
        )

    def _recover_submissions_bronze(
        self,
        normalized_cik: str,
        location: BronzeRunLocation,
    ) -> SecBronzeDatasetResult | None:
        recovered = recover_or_verify_bronze_artifacts(
            location,
            table_from_payload=lambda payload: sec_submissions_to_table(
                parse_submissions(payload, expected_cik=normalized_cik)
            ),
        )
        if recovered is None:
            return None
        return self._recovered_dataset_result(
            normalized_cik,
            location,
            recovered.table.num_rows,
        )

    def _recover_company_facts_bronze(
        self,
        normalized_cik: str,
        location: BronzeRunLocation,
    ) -> SecBronzeDatasetResult | None:
        recovered = recover_or_verify_bronze_artifacts(
            location,
            table_from_payload=lambda payload: sec_company_facts_to_table(
                parse_company_facts(payload, expected_cik=normalized_cik)
            ),
        )
        if recovered is None:
            return None
        return self._recovered_dataset_result(
            normalized_cik,
            location,
            recovered.table.num_rows,
        )

    def _ingest_submissions_to_bronze(
        self,
        normalized_cik: str,
        location: BronzeRunLocation,
    ) -> SecBronzeDatasetResult:
        payload, submissions = self._fetch_and_parse_submissions(normalized_cik)
        table = sec_submissions_to_table(submissions)
        persisted_raw_json_path = write_raw_json(location, payload)
        persisted_parquet_path = write_parquet(location, table)
        return SecBronzeDatasetResult(
            cik=normalized_cik,
            location=location,
            raw_json_path=persisted_raw_json_path,
            parquet_path=persisted_parquet_path,
            record_count=len(submissions.filings),
        )

    def _ingest_company_facts_to_bronze(
        self,
        normalized_cik: str,
        location: BronzeRunLocation,
    ) -> SecBronzeDatasetResult:
        payload, company_facts = self._fetch_and_parse_company_facts(normalized_cik)
        table = sec_company_facts_to_table(company_facts)
        persisted_raw_json_path = write_raw_json(location, payload)
        persisted_parquet_path = write_parquet(location, table)
        return SecBronzeDatasetResult(
            cik=normalized_cik,
            location=location,
            raw_json_path=persisted_raw_json_path,
            parquet_path=persisted_parquet_path,
            record_count=len(company_facts.facts),
        )

    def _fetch_and_parse_submissions(
        self,
        normalized_cik: str,
    ) -> tuple[dict[str, Any], SecSubmissions]:
        payload = self._request(self._client.fetch_submissions, normalized_cik)
        submissions = parse_submissions(payload, expected_cik=normalized_cik)
        return payload, submissions

    def _fetch_and_parse_company_facts(
        self,
        normalized_cik: str,
    ) -> tuple[dict[str, Any], SecCompanyFacts]:
        payload = self._request(self._client.fetch_company_facts, normalized_cik)
        company_facts = parse_company_facts(payload, expected_cik=normalized_cik)
        return payload, company_facts

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
        _, submissions = self._fetch_and_parse_submissions(normalized_cik)
        return submissions

    def _ingest_company_facts(self, normalized_cik: str) -> SecCompanyFacts:
        _, company_facts = self._fetch_and_parse_company_facts(normalized_cik)
        return company_facts

    def _ingest_company(self, normalized_cik: str) -> SecCompanySourceData:
        submissions = self._ingest_submissions(normalized_cik)
        company_facts = self._ingest_company_facts(normalized_cik)
        return SecCompanySourceData(
            cik=normalized_cik,
            submissions=submissions,
            company_facts=company_facts,
        )
