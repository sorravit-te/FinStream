# Power BI Runtime Validation

## Final Status

- Runtime validation dates: 2026-09-26 and 2026-09-27.
- Canonical project: `powerbi/FinStream.pbip`.
- Overall result: **PASS WITH ONE NON-BLOCKING SOURCE/DISPLAY LIMITATION**.
- The canonical project reopened without reconstruction, refreshed successfully,
  retained all three report pages, and returned the expected imported row
  counts and checked values.
- The supplied Desktop results are the authority for runtime behavior. Static
  checks in this repository validate the serialized PBIP/PBIR/TMDL structure;
  they do not substitute for Power BI Desktop.
- The non-blocking limitation is that the manual runtime checklist reports a
  Unit slicer on Company Financial, while the final serialized PBIR contains
  unit only in the detail table and contains three slicers: Company,
  `MetricShortName`, and Reporting Period. The current imported profile is
  single-unit (`USD`), and the DAX guard still requires one effective unit, so
  validated results are unaffected. A future multi-unit profile requires a
  single-select Unit slicer before it is considered operational.

## Connection and Source Boundary

Power BI Desktop connected in Import mode to the local PostgreSQL development
runtime:

| Setting | Value |
| --- | --- |
| Host endpoint | `localhost:5433` |
| Database | `finstream` |
| Schema | `analytics` |
| Transport | Local development connection with TLS disabled |
| Credentials | Entered only through Power BI's local credential flow; not stored in the repository |

The semantic model imports only these approved analytics marts:

1. `analytics.mart_company_daily_performance`
2. `analytics.mart_company_financial_growth`
3. `analytics.mart_market_macro`

Static source inspection found no Bronze, `source_data`, provenance/control,
Silver, dbt dimension/fact, seed, or other lower-layer analytical source in the
Power BI model.

## PostgreSQL Provider Compatibility

The original Power Query provider failure was:

```text
DataSource.Error: Numeric value does not fit in a System.Decimal
```

The failure occurred while the provider materialized high-precision PostgreSQL
`NUMERIC` values, before a post-Navigation type step could run. PostgreSQL and
dbt precision remain authoritative and were not changed.

The final model uses read-only server-side compatibility projections over the
same approved marts:

- Financial query: casts only `current_value`, `previous_value`,
  `absolute_change`, and `growth_rate` to `double precision`.
- Market--macro query: casts only `macro_value` to `double precision`.
- Daily market query: retains the normal Navigator import and performs no
  compatibility cast.

Both native queries use explicit column lists, preserve aliases, grain, nulls,
and business logic, and do not cast identifiers or relationship keys.

## Refresh and Imported Row Counts

The recovered project reopened successfully. After it replaced the stale
canonical project, the canonical `powerbi/FinStream.pbip` also reopened without
reconstruction and refreshed successfully.

| Imported table | PostgreSQL rows | Power BI rows after canonical refresh | Result |
| --- | ---: | ---: | --- |
| `analytics mart_company_daily_performance` | 204 | 204 | PASS |
| `analytics mart_company_financial_growth` | 562 | 562 | PASS |
| `analytics mart_market_macro` | 1,020 | 1,020 | PASS |

## Semantic Model Inventory

The reopened and refreshed model contains six tables:

- `_Measures`
- `DimCompany`
- `DimDate`
- `analytics mart_company_daily_performance`
- `analytics mart_company_financial_growth`
- `analytics mart_market_macro`

`DimCompany` contains `CompanyKey`, `CurrentTicker`, `CurrentCIK`, and
`CompanyName`. It is based on the distinct union of approved-mart company keys
and is enriched with the available descriptive attributes.

`DimDate` covers the union of required market, financial, filing, prior-period,
and macro reference-date roles. It is configured as the model's time table and
has sort-by mappings for YearMonth, MonthName, YearQuarter, and WeekdayName.
Automatic time intelligence is disabled for the model.

The financial fact includes calculated column `MetricShortName`. It is a
presentation-only label helper; `metric_key` remains canonical and the original
`metric_name` remains in the model.

### Measures

The `_Measures` table contains 42 measures:

- Market Performance (18): `Close`, `Daily Return`, `Daily Volume`, `Latest
  Trading Date`, `Latest Close`, `Previous Trading Close`, `Daily Change`,
  `Daily Change %`, `Period Start Close`, `Period End Close`, `Selected Period
  Return`, `Average Daily Return`, `Minimum Close`, `Maximum Close`, `Average
  Close`, `Total Volume`, `Average Daily Volume`, and `Trading Days`.
- Company Financial (12): `Financial Context Is Valid`, `Financial Value`,
  `Growth Rate`, `Latest Reporting Period`, `Prior Comparable Value`, `Latest
  Financial Value`, `Latest Growth Rate`, `Latest Filed Date`, `Financial
  Periods`, `Minimum Financial Value`, `Maximum Financial Value`, and `Average
  Financial Value`.
- Market & Macro (12): `Market Macro Context Is Valid`, `Market Close`, `Macro
  Value`, `Macro Observation Date`, `Macro Observation Age`, `Market Macro
  Latest Trading Date`, `Latest Market Close`, `Latest Aligned Macro Value`,
  `Latest Macro Observation Date`, `Latest Macro Observation Age`, `Market
  Macro Trading Days`, and `Distinct Macro Observation Dates`.

The macro-specific names resolve the collision with the Market Performance
helpers while preserving `Latest Trading Date` and `Trading Days` for the
market page.

### Relationships

The model contains eight one-to-many, single-direction relationships: six
active and two inactive.

| Dimension role | Fact role | State |
| --- | --- | --- |
| `DimCompany[CompanyKey]` | Daily `company_key` | Active |
| `DimCompany[CompanyKey]` | Financial `company_key` | Active |
| `DimCompany[CompanyKey]` | Market--macro `company_key` | Active |
| `DimDate[Date]` | Daily `trading_date` | Active |
| `DimDate[Date]` | Financial `end_date` | Active |
| `DimDate[Date]` | Market--macro `trading_date` | Active |
| `DimDate[Date]` | Financial `filed_date` | Inactive |
| `DimDate[Date]` | Market--macro `macro_observation_date` | Inactive |

There is no fact-to-fact, bidirectional, or many-to-many relationship.

## Dashboard Inventory

All three pages persisted across recovered and canonical reopen.

### Market Performance

The page contains 14 serialized visual containers: Company and Date slicers,
five KPI cards, Close Over Time, Daily Return, Daily Volume, Company Period
Return, a detail table, title text, and one layout container.

- Daily Return uses red `#C25A3D` for negative values; non-negative values use
  the dashboard's default positive color.
- Company Period Return applies the same negative-color rule, sorts by Selected
  Period Return descending, and has an explicit `NoFilter` interaction from the
  Company slicer. The selected Date range still applies, so the chart can show
  all companies while the rest of the page focuses on one company.
- The detail table contains `DimDate[Date]`, `DimCompany[CurrentTicker]`, open,
  high, low, close, volume, and `[Daily Return]`.

### Company Financial

The page contains 15 serialized visual containers: Company,
`MetricShortName`, and Reporting Period slicers; five KPI cards; Financial
Value Over Time; Growth Rate Over Time; Latest vs Prior Comparable Value; the
detail table; title text; and two layout containers.

- `MetricShortName` is used only for concise display labels.
- Growth Rate Over Time uses red `#C25A3D` for negative values and the default
  positive color for non-negative values. The manually validated visual has a
  zero reference line.
- Latest vs Prior Comparable Value displays the two guarded measures directly;
  it has no metric category field.
- The detail table contains exactly `current_ticker`, `MetricShortName`, `unit`,
  `end_date`, `current_value`, `previous_value`, `absolute_change`,
  `growth_rate`, `filed_date`, `form`, and `accession_number`.
- The serialized-source Unit-slicer discrepancy is recorded in Final Status;
  no unvalidated PBIR visual was fabricated during finalization.

### Market & Macro

The page contains 17 serialized visual containers: Company, `series_id`, and
Trading Date Range slicers; five KPI cards; Market Close Over Time; Aligned
Macro Value Over Time; Macro Observation Age Over Time; Aligned Reference-Date
Observation Trend; the detail table; title text; and three layout containers.

- The reference-date chart groups by `macro_observation_date` and plots
  `[Market Macro Trading Days]`, showing the number of aligned market sessions
  represented by each reference date.
- The detail table contains exactly `trading_date`, `current_ticker`,
  `series_id`, `market_close`, `market_volume`, `macro_observation_date`,
  `macro_value`, `macro_age_days`, and `current_cik`.
- Dates selected for presentation use `dd/MM/yyyy`.

The page remains explicitly current-vintage and reference-date aligned.
`macro_observation_date` is not a release/publication date, and
`macro_age_days` is reference-date age rather than publication or information
lag. The model makes no vintage-awareness, release-awareness, no-lookahead,
causality, forecasting, or investor-knowledge claim. Market close and volume
repeat by series and are not additive across series. `series_id` is the only
approved macro selector; the mart supplies no unit/title/frequency metadata.

## PostgreSQL-to-Power-BI Numeric Validation

### AAPL Market Performance

| Value | PostgreSQL | Power BI |
| --- | ---: | ---: |
| Trading date | 2026-09-24 | 2026-09-24 |
| Latest close | 335.920010000000000000 | 335.920010 |
| Previous trading close | 337.019989000000000000 | 337.019989 |
| Daily change | -1.099979000000000000 | -1.099979 |
| Daily return | -0.00326383904783760467 | -0.003263839047837600 |
| Displayed daily return | n/a | -0.326384% |

Result: **PASS**. The difference is limited to expected floating-point
representation at deep decimal precision.

### AAPL Financial: `total_assets`, `USD`

| Value | PostgreSQL | Power BI |
| --- | ---: | ---: |
| Reporting end date | 2025-09-27 | 2025-09-27 |
| Current value | 359241000000.000000000000000000000000000000 | 359241000000 |
| Prior comparable value | 364980000000.000000000000000000000000000000 | 364980000000 |
| Growth rate | -0.015724149268453065921420351800 | -0.015724149268453100 |
| Displayed growth rate | n/a | -1.572415% |
| Filed date | 2025-10-31 | 2025-10-31 |

Result: **PASS**. The integer financial values match exactly. Growth differs
only at expected deep floating-point precision introduced by the compatibility
projection.

### AAPL Market & Macro: `DFF`

| Value | PostgreSQL | Power BI |
| --- | ---: | ---: |
| Trading date | 2026-09-24 | 2026-09-24 |
| Market close | 335.920010000000000000 | 335.920010 |
| Macro value | 3.880000000000000000000000000000 | 3.880000000000000000 |
| Macro observation date | 2026-09-23 | 2026-09-23 |
| Macro observation age | 1 | 1 |

Result: **PASS**.

## Precision Findings

- Current financial whole-number values are below binary64's exact-integer
  limit and the checked values remained exact.
- Financial `growth_rate` retains source meaning but cannot preserve all of the
  PostgreSQL decimal's deep fractional digits after the required compatibility
  cast. This is represented and formatted as a Power BI Decimal Number.
- The checked `macro_value` retained its meaningful source precision; binary64
  storage remains approximate in principle.
- PostgreSQL/dbt values remain authoritative. Display rounding and compact
  formatting are presentation behavior, not source rounding.

## Canonical Project Recovery and PBIX Retirement

The previous canonical PBIP source was stale and did not represent the
validated report. The validated PBIX was treated as the authoritative recovery
input and converted to a Power BI Project. The recovered project reopened
successfully before canonical replacement. Its report/model references and
display identities were normalized to `FinStream`, then it replaced the stale
canonical project.

The stale canonical source was preserved outside the repository at:

```text
C:\Users\acer\Desktop\finstream-power-bi-stale-backup-20260927
```

That backup contains 31 source files plus `SHA256SUMS.txt` and was verified
byte-for-byte. It remained untouched during finalization.

After the new canonical PBIP passed reopen, refresh, row-count, and numeric
validation, the repository PBIX was retired as a competing source of truth. The
original untracked 277,197-byte PBIX was copied to:

```text
C:\Users\acer\Desktop\finstream-power-bi-pbix-backup-20260927\FinStream.pbix
```

Its SHA-256 is:

```text
33FDC1D449CCE322A539EAD3892CE31EA6CBA0FB337291A9705F6C421A6E98F9
```

The backup hash matched the source byte-for-byte and is recorded in the
external backup's `SHA256SUMS.txt`. `powerbi/FinStream.pbix` was then removed
from the repository working tree. The external backup was not deleted.

`powerbi_recovery/` is absent, and the canonical source contains no legacy
recovered-name reference.

## Static Validation and Hygiene

Repository-only validation produced these results:

- 64 JSON/PBIP/PBIR/PBISM/platform files parsed successfully.
- The PBIP report reference resolves to `FinStream.Report`; the PBIR semantic
  model reference resolves to `../FinStream.SemanticModel`.
- Both report and semantic-model platform display names are `FinStream`.
- Page inventory: 3 pages with 14, 15, and 17 serialized visual containers.
- Semantic inventory: 6 tables, 42 measures, and 8 relationships (6 active,
  2 inactive).
- Source queries reference only the three approved marts and contain only the
  documented compatibility casts.
- No stale recovered-name reference, credential, password, API key, secret,
  lower-layer analytical source, or unintended repository binary was found.
- `.pbi/localSettings.json` and `.pbi/cache.abf` are ignored; PBIP, PBIR, TMDL,
  report, and model source files are not ignored.
- `git diff --check HEAD` passes for the tracked-file delta.
- Power BI-generated TMDL currently contains 61 trailing-whitespace findings:
  4 in `DimCompany.tmdl`, 4 in `DimDate.tmdl`, 45 in `_Measures.tmdl`, and 8 in
  the financial mart TMDL. These runtime-validated generated files were not
  reformatted for whitespace aesthetics.
- The complete cumulative patch's strict whitespace check reports 71 generated-
  source findings: the 61 trailing-whitespace findings plus 10 new blank lines
  at end of file across generated TMDL/DAX files. They are preserved separately
  from the clean tracked-text result.

The canonical Power BI runtime implementation is validated. The Unit-slicer
serialization discrepancy and generated TMDL whitespace are documented,
non-blocking limitations; neither changes the successful current-profile
runtime results.
