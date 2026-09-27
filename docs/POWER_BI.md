# Power BI

Open [FinStream.pbip](../powerbi/FinStream.pbip) in Power BI Desktop. The PBIP
project contains the report and semantic model; it is the source of truth for
visual definitions and DAX.

## Approved Relations

| Relation | Purpose |
| --- | --- |
| `analytics.mart_company_daily_performance` | Company market performance. |
| `analytics.mart_company_financial_growth` | Reported financial values and growth. |
| `analytics.mart_market_macro` | Market and macroeconomic analysis. |

The semantic model also contains company/date dimensions and measure tables.
Use CIK and current ticker context consistently; preserve source units and
financial filing provenance in drill-through or tooltips.

## Dashboards

### Market Performance

![Market Performance dashboard](assets/dashboard-market.jpg)

Daily OHLC, returns, volume, and company context for the selected reporting period.

### Company Financial

![Company Financial dashboard](assets/dashboard-financial.jpg)

Reported financial values, fiscal periods, units, filing provenance, and comparable growth for the selected company and metric.

### Market & Macro

![Market and Macro dashboard](assets/dashboard-market-macro.jpg)

Market values with macro observations aligned to each market reference date and their observation age.

Macro values are current-vintage and reference-date aligned. They are not
release-aware, vintage-history, interpolated, or forward-looking values.

## Refresh

Configure the PostgreSQL source to the local analytics database, refresh after
dbt validation, and confirm the three approved marts resolve. Power BI consumes
analytics relations only; it does not query Bronze or `source_data` directly.
