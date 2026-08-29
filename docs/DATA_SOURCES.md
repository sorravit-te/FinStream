# Data Sources

## Overview

FinStream V1 combines the following external sources:

| Domain | Provider | Purpose |
| --- | --- | --- |
| Corporate financial data | SEC EDGAR | Public-company filings and structured financial facts |
| Market data | Twelve Data | Daily OHLCV market data |
| Macroeconomic data | FRED | Economic indicators |

Each source is ingested independently and normalized later, preventing provider-specific formats from leaking into downstream analytical models.

## SEC EDGAR

### Purpose

SEC EDGAR provides corporate filing metadata and structured XBRL financial facts for AAPL (Apple), MSFT (Microsoft), NVDA (NVIDIA), AMZN (Amazon), XOM (Exxon Mobil), and WMT (Walmart).

### Data Used

FinStream initially uses filing history and metadata, structured XBRL company facts, and commonly useful reported values: revenue, net income, assets, liabilities, cash, stockholders' equity, and earnings per share. Final XBRL concept mappings are deferred to later source implementation and data-modeling work.

### Access and Format

SEC public data APIs return JSON and do not require an API key. Automated access must comply with SEC fair-access rules and identify the application with an appropriate User-Agent. SEC currently publishes a maximum fair-access rate of 10 requests per second; FinStream should operate comfortably below that limit rather than maximize throughput.

### Update Behavior

SEC data is filing-driven, not daily. Quarterly and annual filings, amendments, and other filing events can update submissions and XBRL APIs as filings are disseminated. FinStream must detect new or changed source records rather than assume each run has new financial data.

### Source Considerations

- Companies may use different XBRL concepts, reported units, and fiscal calendars.
- Amended filings or restatements may change previously observed information.
- Source concepts must be normalized before cross-company analytics.

## Twelve Data

### Purpose

Twelve Data provides FinStream V1's daily market-price data.

### Data Used

The initial dataset contains daily OHLCV fields: symbol, trading date, open, high, low, close, and volume.

### Access and Format

Twelve Data requires an API key. Its responses will be parsed into FinStream's internal market schema. Twelve Data uses a credit-based quota system; the current Basic plan provides 8 API credits per minute and 800 credits per day. These limits are external operational constraints and may change.

### Update Behavior

FinStream consumes daily market observations. Weekends and exchange holidays do not normally produce new rows, so a pipeline run does not necessarily create a market record. Incremental ingestion should retrieve only required new or missing trading dates where practical.

### Source Considerations

- Ingestion must account for API quota and rate limits.
- Market holidays, repeated runs, and duplicate prevention affect incremental processing.
- Provider failures must be distinguishable from legitimate empty or no-new-data results.
- V1 does not include real-time WebSocket processing.

## FRED

### Purpose

FRED provides macroeconomic data that adds broader economic context to market and company data. Initial series are DFF (Federal Funds Effective Rate), CPIAUCSL (Consumer Price Index), UNRATE (Unemployment Rate), GDPC1 (Real Gross Domestic Product), and DGS10 (10-Year Treasury Constant Maturity Rate).

### Access and Format

FRED API access requires an API key, and FinStream will request JSON responses. Series metadata and observations should remain distinguishable because frequency, units, and update behavior vary by series.

### Update Behavior

The initial series include daily, monthly, and quarterly observations; no series should be assumed to update on every ingestion run. Economic values may be revised. FRED exposes real-time or vintage metadata, including `realtime_start` and `realtime_end`; V1 records this source behavior without defining a final revision-history model.

### Source Considerations

- Missing observations can be valid source values.
- Observation dates can differ from publication or update timing.
- Historical values may be revised, and series can use different units and seasonal-adjustment metadata.
- These characteristics must inform incremental ingestion.

## Source Boundary

This document owns provider characteristics, authentication requirements, source formats, update behavior, and source-specific operational constraints and limitations.

It does not own system component responsibilities (`ARCHITECTURE.md`), project scope and goals (`PROJECT_REQUIREMENTS.md`), analytical table or model design (`DATA_MODEL.md`), or implementation sequencing (`ROADMAP.md`).
