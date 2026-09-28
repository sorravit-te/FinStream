# FinStream

<p align="center">
  <img src="https://s3.dualstack.us-east-2.amazonaws.com/pythondotorg-assets/media/files/python-logo-only.svg" height="42" alt="Python" title="Python">
  &nbsp;&nbsp;&nbsp;
  <img src="https://www.postgresql.org/media/img/about/press/elephant.png" height="42" alt="PostgreSQL" title="PostgreSQL">
  &nbsp;&nbsp;&nbsp;
  <img src="https://raw.githubusercontent.com/dbt-labs/docs.getdbt.com/current/website/static/img/icons/dbt-bit.svg" height="42" alt="dbt" title="dbt">
  &nbsp;&nbsp;&nbsp;
  <img src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_64x64_emoji_transparent.png" height="42" alt="Apache Airflow" title="Apache Airflow">
  &nbsp;&nbsp;&nbsp;
  <img src="https://www.docker.com/app/uploads/2024/02/cropped-docker-logo-favicon-192x192.png" height="42" alt="Docker" title="Docker">
  &nbsp;&nbsp;&nbsp;
  <img src="https://raw.githubusercontent.com/microsoft/PowerBI-Icons/main/SVG/Power-BI.svg" height="42" alt="Power BI" title="Power BI">
</p>

FinStream is a batch data platform for SEC EDGAR filings, Twelve Data daily
prices, and FRED macroeconomic series. It preserves source data, builds
PostgreSQL analytics marts with dbt, orchestrates manual runs with Airflow, and
exposes the marts through Power BI.

**Coverage:** AAPL, MSFT, NVDA, AMZN, XOM, WMT | FRED: DFF, CPIAUCSL, UNRATE,
GDPC1, DGS10

## Architecture

[![FinStream architecture](docs/assets/finstream-architecture.png)](docs/assets/finstream-architecture.png)

## Highlights

- Immutable Bronze artifacts with source, dataset, run, and row provenance.
- PostgreSQL constraints and replay checks preserve valid source revisions.
- dbt models and tests produce documented Silver, Gold, and mart relations.
- Docker Compose runs PostgreSQL and Airflow locally. Airflow provides a manual
  full pipeline and a weekday incremental Market pipeline.
- GitHub Actions validates Python tests, PostgreSQL integration, and dbt checks
  on pushes to main and pull requests.
- Read-only operational status reports database, provenance, Bronze, analytics,
  and informational recency state.

## Automation

| Workflow | Schedule | Scope |
| --- | --- | --- |
| `finstream_market_daily` | Monday-Friday, 18:30 `America/New_York` | Market-only incremental ingestion, followed by dbt run, dbt tests, and quality monitoring. |
| `finstream_v1_pipeline` | Manual trigger | Complete Market, SEC, and FRED workflow. |
| Power BI scheduled refresh | Tuesday-Saturday, 08:00 ICT (UTC+7) | Scheduled after the expected prior U.S. trading-day pipeline completion, with buffer. |

The daily Market DAG uses `catchup=False`: missed Airflow runs are not
recreated. Instead, ingestion starts from each symbol's latest stored trading
date minus three calendar days and requests through the provider's latest date,
recovering missing observations and recent revisions within the overlap window.
SEC and FRED are not fetched by the daily DAG.

## Quick Start

Use the repository virtual environment and follow the concise operational
commands in the [Runbook](docs/RUNBOOK.md). The dependency source of truth is
`pyproject.toml`; no `requirements.txt` is used.

## Dashboard Preview

These Power BI pages present the approved marts; see the [Power BI guide](docs/POWER_BI.md) for their data contract and refresh guidance.

<p align="center">
  <a href="docs/assets/dashboard-market.jpg">
    <img src="docs/assets/dashboard-market.jpg" width="32%" alt="Market Performance dashboard">
  </a>
  <a href="docs/assets/dashboard-financial.jpg">
    <img src="docs/assets/dashboard-financial.jpg" width="32%" alt="Company Financial dashboard">
  </a>
  <a href="docs/assets/dashboard-market-macro.jpg">
    <img src="docs/assets/dashboard-market-macro.jpg" width="32%" alt="Market and Macro dashboard">
  </a>
</p>

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Data Sources](docs/DATA_SOURCES.md)
- [Data Model](docs/DATA_MODEL.md)
- [Data Quality](docs/DATA_QUALITY.md)
- [Power BI](docs/POWER_BI.md)
- [Runbook](docs/RUNBOOK.md)
