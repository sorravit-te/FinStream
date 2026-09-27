# Data Model

## Layers

| Layer | Relations | Purpose |
| --- | --- | --- |
| Source | `source_data` tables | Append-oriented source rows and ingestion provenance. |
| Silver | `stg_market_prices`, `stg_financial_facts`, `stg_macro_observations` | Typed, source-aligned, latest-representation staging. |
| Gold | dimensions and facts | Curated analytical entities and relationships. |
| Marts | company daily, financial growth, market macro | Consumer-facing Power BI relations. |

`(source, dataset, run_id)` identifies one ingestion run. `source_row_number`
preserves source order. A later run may legitimately represent a revision of the
same business record; replay validation is exact-run scoped.

## Grains and Identity

- Market source identity is `(symbol, trading_date)` within a run. Silver keeps
  the latest representation using ingestion provenance tie-breakers.
- FRED source identity is `(series_id, observation_date, realtime_start,
  realtime_end)`. `NULL` remains a valid missing observation value.
- SEC submissions use `(cik, accession_number)` as represented identity;
  Company Facts retain their occurrence context and nullable instant starts.
- CIK is the durable company identity. Current ticker mappings support the
  configured companies while predecessor/successor CIK provenance is retained.

## Analytics Marts

| Mart | Grain | Use |
| --- | --- | --- |
| `analytics.mart_company_daily_performance` | Company and trading date | Daily OHLC and derived market measures. |
| `analytics.mart_company_financial_growth` | Company, mapped metric, unit, reporting period | Representative financial facts and consecutive-period growth. |
| `analytics.mart_market_macro` | Company, trading date, macro series | Market values aligned with macro observations. |

`mart_market_macro` is current-vintage and aligned by observation/reference
date. It is not release-time aware, ALFRED-vintage aware, or a historical
no-lookahead reconstruction. Reusing a lower-frequency observation across later
trading dates is reference-date alignment, not interpolation or imputation.
