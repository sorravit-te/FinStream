# Data Quality

## Purpose

This contract defines FinStream's current data-quality responsibilities and the
rules that may be added without changing source semantics. Quality means that
data can be represented, traced, and used at its documented grain; it does not
mean that every unusual provider value is invalid.

FinStream distinguishes four cases:

- **Invalid or structural data** cannot meet a required source or storage
  contract and is rejected.
- **Suspicious data** may be valid, so it is a future monitoring concern rather
  than an ingestion failure.
- **A source revision** is a legitimate later representation of the same
  source identity and remains append-oriented across distinct runs.
- **Missing or incomplete data** is assessed only against an implemented source
  scope. It is not inferred from a maximum date or an absent event alone.

The incremental and replay semantics in
[`INCREMENTAL_IDEMPOTENCY.md`](INCREMENTAL_IDEMPOTENCY.md) remain authoritative:
quality rules must not overwrite immutable Bronze artifacts, turn valid
cross-run revisions into duplicates, or repair inconsistent committed state.

## Quality Ownership

| Layer | Owns | Does not own |
| --- | --- | --- |
| Source parsing / Python | Required provider shape, required identities, type and date parsing, and source-specific structural invariants needed for safe representation. | Statistical outliers, global cross-run business-key uniqueness, or source-history completeness. |
| Bronze | Strict Raw JSON and Arrow/Parquet schemas, deterministic row counts, and immutable artifact recovery verification. | Analytical filtering, imputation, or deletion of valid source values. |
| PostgreSQL source layer | Run provenance, per-run keys, foreign keys, check constraints, exact-run replay verification, and schema/count preflight. | Cross-run uniqueness that would discard revisions or automatic repair of inconsistent committed state. |
| dbt source, Silver, and Gold | Persisted required analytical fields, latest-representation grains, modeled relationships, curated mapping consistency, and consumer-facing fact/dimension grains. | Re-validating every provider parser rule or treating unmapped facts and valid nullable source values as failures. |
| Monitoring (future) | Source-specific freshness, row-count movement, and anomaly signals with baselines and operational routing. | A universal freshness threshold, exchange-calendar completeness, or automatic repair. |

## Dataset Contracts

### Twelve Data daily market prices

- The source identity is `(symbol, trading_date)`; the source parser requires a
  matching daily response symbol, ISO trading date, finite decimal OHLC values,
  and no duplicate date in one payload.
- `high >= low`, `high >= open`, `high >= close`, `low <= open`, and `low <=
  close` are current hard invariants. `volume` is nullable but, when supplied,
  must be a non-negative integer.
- PostgreSQL repeats these market-domain checks and prevents duplicate market
  identities within one ingestion run. Silver and Gold test the documented
  latest `(symbol, trading_date)` grain.
- Prices are not currently constrained to be positive or non-negative. Such a
  rule requires an explicit supported-instrument contract; it is not inferred
  from the current OHLC schema. No exchange calendar exists, so absent dates
  and historical gaps are not quality failures.

### FRED series metadata

- One metadata request must return exactly one record for the requested
  `series_id`. Required metadata strings, timezone-aware `last_updated`,
  non-negative `popularity`, and ordered real-time and observation ranges are
  source validation requirements.
- Bronze and PostgreSQL require one typed metadata row for the series in an
  ingestion run; PostgreSQL also enforces the two ordered date ranges.
- Units, frequency, and seasonal-adjustment labels are source-specific text,
  not candidates for global accepted-value lists. Metadata is fully refreshed;
  it is not a watermark or a completeness assertion for observations.

### FRED observations

- The represented occurrence identity is `(series_id, observation_date,
  realtime_start, realtime_end)`. The parser requires valid dates, an ordered
  real-time range, and a finite decimal value or the valid FRED `"."` missing
  marker, which is retained as `NULL`.
- PostgreSQL prevents duplicate represented occurrences within one run; Silver
  tests the same latest-representation grain while preserving distinct
  real-time contexts. Gold deliberately selects the latest context per
  `(series_id, observation_date)`.
- The parser rejects a duplicate represented occurrence in one payload before
  Bronze persistence, even if values differ or one uses the `"."` marker. The
  check is scoped to that payload; a later run may represent the same context.
- No numeric range is valid for all FRED series. Observation dates are not
  currently tested against metadata bounds: separate responses and revision
  timing make this an unapproved analytical candidate, not a present failure.

### SEC submissions

- The implemented scope is the provider's `filings.recent` response, with
  `(cik, accession_number)` as the represented identity.
- Parsing normalizes and verifies the expected CIK, validates aligned recent
  arrays, accession format, required filing date/form/company fields,
  non-negative size, 0/1 XBRL flags, optional fields, and duplicate accessions
  within one payload. PostgreSQL enforces normalized CIK shape, required
  fields, non-negative size, and per-run accession uniqueness.
- Optional report dates, acceptance timestamps, documents, and fiscal-like
  fields must retain their source nullability. Form-specific date or document
  relationships are not universal invariants.
- `filings.recent` cannot establish complete filing history or source
  disappearance, so neither is a quality failure.

### SEC Company Facts

- The current Silver analytical deduplication tuple is `(cik, taxonomy,
  concept, unit, start_date, end_date, accession_number, fiscal_year,
  fiscal_period, form, filed_date, frame)`. It is not a proven globally
  complete XBRL occurrence identity.
- Parsing validates the expected normalized CIK, entity/taxonomy/concept/unit,
  finite numeric value, accession format, required end and filed dates,
  required form, optional fiscal/frame fields, and `start_date <= end_date`
  when a start exists. Negative and zero values remain valid source values.
- Bronze schemas and PostgreSQL retain nullable instant-fact starts and enforce
  the date-range constraint. Silver tests its documented analytical grain;
  Gold only tests mapped facts and leaves unmapped facts valid in Silver.
- No global Company Facts uniqueness, universal metric range, amendment
  rejection, or accession-to-submission relationship is imposed. The source
  representation lacks the context and dimensional qualifiers needed to prove
  such rules safely.

## Quality Rule Matrix

| Dataset | Rule | Dimension | Layer | Severity | Current / missing |
| --- | --- | --- | --- | --- | --- |
| All loaded datasets | Canonical source/dataset/run identity, paths, non-negative count, and exact replay metadata/count | Validity, referential integrity | PostgreSQL | Reject | Current |
| All Bronze runs | Strict JSON, typed Arrow schema, deterministic count, and complete-pair equivalence | Validity, consistency | Bronze | Reject | Current |
| Market | Required matching daily symbol/date, finite OHLC, and unique date per payload | Validity, uniqueness | Source parsing | Reject | Current |
| Market | OHLC bounds and nullable non-negative volume | Consistency, domain | Source parsing and PostgreSQL | Reject | Current |
| Market | Latest `(symbol, trading_date)` representation | Uniqueness | dbt Silver / Gold | Test failure | Current |
| Market | Positive-price rule or trading-calendar gap detection | Domain, completeness | Not assigned | Informational | Not justified without an instrument/calendar contract |
| FRED metadata | Exactly one requested series, required fields, ordered dates, non-negative popularity | Validity, completeness, consistency | Source parsing and PostgreSQL | Reject | Current |
| FRED observations | Valid dates, ordered real-time context, finite decimal or `"."` -> `NULL` | Validity, consistency | Source parsing and PostgreSQL | Reject | Current |
| FRED observations | Duplicate represented occurrence in one payload | Uniqueness | Source parsing and PostgreSQL | Reject | Current |
| FRED observations | Latest `(series_id, observation_date, realtime_start, realtime_end)` representation | Uniqueness | dbt Silver | Test failure | Current |
| FRED observations | Universal value range or metadata-date containment | Domain, consistency | Not assigned | Informational | Not justified across all series/revision timing |
| SEC submissions | CIK, aligned arrays, accession/date/form/size/flag structure, and per-payload accession uniqueness | Validity, uniqueness | Source parsing and PostgreSQL | Reject | Current |
| SEC submissions | Complete filing history or disappearance detection | Completeness | Not assigned | Informational | Outside `filings.recent` scope |
| SEC Company Facts | Required structural fields, finite value, valid dates, optional instant start, and ordered duration | Validity, consistency | Source parsing and PostgreSQL | Reject | Current |
| SEC Company Facts | Latest documented analytical tuple | Uniqueness | dbt Silver | Test failure | Current; not a global XBRL identity claim |
| Gold facts/dimensions | Required consumer keys, fact grains, modeled CIK/metric/series relationships, and fact dates in `dim_date` | Completeness, uniqueness, referential integrity | dbt Gold | Test failure | Current |
| Financial metric mapping | Required seed columns, unique taxonomy/concept mapping, and consistent metric names | Validity, uniqueness, consistency | dbt seed / singular tests | Test failure | Current |
| All sources | Freshness, row-count movement, null-rate movement, and extreme-value signals | Freshness, anomaly | Monitoring | Warning | Missing; no thresholds, baselines, or routing yet |

## Failure and Warning Policy

- **Reject ingestion** only when a record cannot safely meet a required source,
  Bronze, or physical-storage contract. Parsing and storage checks must retain
  valid nulls, source missing markers, instant facts, and revisions.
- **Fail a dbt test** when a persisted analytical model violates its documented
  key, grain, curated mapping, or modeled relationship. dbt must test the
  latest-representation semantics, not demand cross-run source uniqueness.
- **Warn or report** source-specific freshness, unexpected counts/null rates,
  and unusual values only after a source-aware baseline and destination are
  approved. Warnings do not mutate Bronze, PostgreSQL history, or source data.
- **Leave out of scope** rules that require an exchange calendar, complete SEC
  history, global XBRL identity, universal FRED value domains, automated
  repair, imputation, or deletion of historical representations.

## Freshness, Completeness, and Anomalies

No automated freshness or anomaly detector exists today. A shared threshold
would be incorrect: Market availability depends on trading days, FRED on
series publication frequency, and SEC on filing events. Likewise, a maximum
date does not prove historical completeness, and `filings.recent` is an
intentional bounded SEC scope.

Future monitoring may report source-specific row-count changes, staleness, or
unusual values, but requires an explicit baseline, schedule, and operational
owner. It must not classify a valid source correction, no-new-data response,
or nullable FRED value as invalid.

## Known Limitations and Non-goals

- Market date coverage cannot be judged without an exchange calendar.
- FRED's finite retrieval overlap cannot prove full revision history or every
  historical gap.
- SEC submissions cover `filings.recent`, not all supplemental files; source
  removals are not inferred.
- Company Facts do not expose a globally complete XBRL occurrence identity in
  the represented schema.
- This contract does not add dashboards, alerting, scheduler integration,
  statistical/ML anomaly detection, automatic repair, imputation, new
  watermarks, cross-run business-key uniqueness, or dbt incremental models.
