# Project Requirements

## Project Overview

FinStream is an end-to-end batch Data Engineering project that combines corporate financial data from SEC EDGAR, daily market data from Twelve Data, and macroeconomic data from FRED. It is intended to produce reliable, analytics-ready datasets; it is not a stock prediction or trading project.

## Goals

- Ingest corporate financial, daily market, and macroeconomic data from the three initial providers.
- Preserve source data before downstream transformation.
- Standardize heterogeneous data into consistent internal schemas.
- Support incremental processing and safe reruns without duplicate records.
- Validate important data quality rules and create analytics-ready datasets that combine the three domains.
- Eventually provide outputs suitable for Power BI and remain reproducible and understandable to another developer.
- Eventually provide outputs suitable for Power BI reporting/dashboards and Streamlit interactive analytical consumption, and remain reproducible and understandable to another developer.

## Initial Scope

The initial financial scope covers AAPL (Apple), MSFT (Microsoft), NVDA (NVIDIA), AMZN (Amazon), XOM (Exxon Mobil), and WMT (Walmart). It includes commonly useful reported values such as revenue, net income, assets, liabilities, cash, stockholders' equity, and earnings per share; final SEC XBRL concept mappings are out of scope for this document.

Market data is limited to daily OHLCV records with symbol, trading date, open, high, low, close, and volume. V1 is batch-oriented and excludes real-time, tick-level, and streaming processing.

The initial macroeconomic scope covers FRED series DFF, CPIAUCSL, UNRATE, GDPC1, and DGS10. The system must accommodate daily, monthly, and quarterly observations without assuming all series update at the same frequency.

## Core Requirements

- Separate ingestion from downstream transformation.
- Preserve raw source data before applying transformations.
- Use standardized internal schemas for data from different providers.
- Load incrementally where the source supports it rather than rebuilding all history.
- Make processing idempotent and prevent duplicate records when runs are repeated.
- Validate important data quality rules.
- Keep transformation logic version controlled and testable.
- Define clear analytical grains for downstream datasets.
- Produce useful logs to investigate failed pipeline runs.

## System Qualities

- API credentials and secrets must never be committed to Git; local configuration must be environment-driven.
- The project must be reproducible on another development machine.
- Failures must produce useful logs rather than silently dropping data.
- Components must have clear responsibilities, and external providers should be replaceable without rewriting unrelated logic.
- Implementation complexity must remain proportional to actual data volume and requirements; unnecessary infrastructure should be avoided.

## Non-Goals

V1 does not include stock price prediction, investment recommendations, trade execution, high-frequency trading, real-time trading infrastructure, Kafka or another streaming platform, Apache Spark, mandatory cloud infrastructure, ingestion of every publicly listed company, or ingestion of every available economic series.

## Success Criteria

A successful V1 can ingest all three data domains, preserve raw source data, standardize heterogeneous records, and transform them into documented analytics-ready models. It validates important quality rules, supports incremental processing, reruns safely without duplicates, and exposes datasets suitable for downstream BI consumption.
A successful V1 can ingest all three data domains, preserve raw source data, standardize heterogeneous records, and transform them into documented analytics-ready models. It validates important quality rules, supports incremental processing, reruns safely without duplicates, and exposes datasets suitable for downstream Power BI reporting/dashboard consumption and Streamlit interactive analytical consumption.
