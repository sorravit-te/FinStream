"""Static PostgreSQL DDL for FinStream's source-aligned loading boundary."""

import psycopg


SOURCE_SCHEMA_NAME = "source_data"

SOURCE_SCHEMA_DDL: tuple[str, ...] = (
    """CREATE SCHEMA IF NOT EXISTS source_data""",
    """CREATE TABLE IF NOT EXISTS source_data.ingestion_runs (
        source TEXT NOT NULL,
        dataset TEXT NOT NULL,
        run_id TEXT NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL,
        raw_json_path TEXT NOT NULL,
        parquet_path TEXT NOT NULL,
        record_count BIGINT NOT NULL,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT pk_ingestion_runs
            PRIMARY KEY (source, dataset, run_id),
        CONSTRAINT ck_ingestion_runs_source_nonblank
            CHECK (btrim(source) <> ''),
        CONSTRAINT ck_ingestion_runs_dataset_nonblank
            CHECK (btrim(dataset) <> ''),
        CONSTRAINT ck_ingestion_runs_run_id_format
            CHECK (run_id ~ '^[0-9]{8}T[0-9]{12}Z$'),
        CONSTRAINT ck_ingestion_runs_raw_path_nonblank
            CHECK (btrim(raw_json_path) <> ''),
        CONSTRAINT ck_ingestion_runs_parquet_path_nonblank
            CHECK (btrim(parquet_path) <> ''),
        CONSTRAINT ck_ingestion_runs_artifact_paths_distinct
            CHECK (raw_json_path <> parquet_path),
        CONSTRAINT ck_ingestion_runs_record_count
            CHECK (record_count >= 0)
    )""",
    """CREATE TABLE IF NOT EXISTS source_data.market_daily_prices (
        source TEXT NOT NULL,
        dataset TEXT NOT NULL,
        run_id TEXT NOT NULL,
        source_row_number BIGINT NOT NULL,
        symbol TEXT NOT NULL,
        trading_date DATE NOT NULL,
        open NUMERIC(38,18) NOT NULL,
        high NUMERIC(38,18) NOT NULL,
        low NUMERIC(38,18) NOT NULL,
        close NUMERIC(38,18) NOT NULL,
        volume BIGINT NULL,
        CONSTRAINT pk_market_daily_prices
            PRIMARY KEY (source, dataset, run_id, source_row_number),
        CONSTRAINT fk_market_daily_prices_run
            FOREIGN KEY (source, dataset, run_id)
            REFERENCES source_data.ingestion_runs (source, dataset, run_id),
        CONSTRAINT ck_market_daily_prices_source_row_number
            CHECK (source_row_number >= 0),
        CONSTRAINT ck_market_daily_prices_identity
            CHECK (
                source = 'twelve_data'
                AND dataset = 'daily_market_prices'
            ),
        CONSTRAINT ck_market_daily_prices_symbol_nonblank
            CHECK (btrim(symbol) <> ''),
        CONSTRAINT ck_market_daily_prices_volume
            CHECK (volume IS NULL OR volume >= 0),
        CONSTRAINT ck_market_daily_prices_high_low
            CHECK (high >= low),
        CONSTRAINT ck_market_daily_prices_high_open
            CHECK (high >= open),
        CONSTRAINT ck_market_daily_prices_high_close
            CHECK (high >= close),
        CONSTRAINT ck_market_daily_prices_low_open
            CHECK (low <= open),
        CONSTRAINT ck_market_daily_prices_low_close
            CHECK (low <= close),
        CONSTRAINT uq_market_daily_prices_run_grain
            UNIQUE (source, dataset, run_id, symbol, trading_date)
    )""",
    """CREATE TABLE IF NOT EXISTS source_data.sec_submissions (
        source TEXT NOT NULL,
        dataset TEXT NOT NULL,
        run_id TEXT NOT NULL,
        source_row_number BIGINT NOT NULL,
        cik TEXT NOT NULL,
        company_name TEXT NOT NULL,
        accession_number TEXT NOT NULL,
        filing_date DATE NOT NULL,
        report_date DATE NULL,
        acceptance_datetime TIMESTAMPTZ NULL,
        form TEXT NOT NULL,
        act TEXT NULL,
        file_number TEXT NULL,
        film_number TEXT NULL,
        items TEXT NULL,
        size BIGINT NOT NULL,
        is_xbrl BOOLEAN NOT NULL,
        is_inline_xbrl BOOLEAN NOT NULL,
        primary_document TEXT NULL,
        primary_doc_description TEXT NULL,
        CONSTRAINT pk_sec_submissions
            PRIMARY KEY (source, dataset, run_id, source_row_number),
        CONSTRAINT fk_sec_submissions_run
            FOREIGN KEY (source, dataset, run_id)
            REFERENCES source_data.ingestion_runs (source, dataset, run_id),
        CONSTRAINT ck_sec_submissions_source_row_number
            CHECK (source_row_number >= 0),
        CONSTRAINT ck_sec_submissions_identity
            CHECK (source = 'sec_edgar' AND dataset = 'submissions'),
        CONSTRAINT ck_sec_submissions_cik
            CHECK (cik ~ '^[0-9]{10}$'),
        CONSTRAINT ck_sec_submissions_company_name_nonblank
            CHECK (btrim(company_name) <> ''),
        CONSTRAINT ck_sec_submissions_accession_nonblank
            CHECK (btrim(accession_number) <> ''),
        CONSTRAINT ck_sec_submissions_form_nonblank
            CHECK (btrim(form) <> ''),
        CONSTRAINT ck_sec_submissions_size
            CHECK (size >= 0),
        CONSTRAINT uq_sec_submissions_run_accession
            UNIQUE (source, dataset, run_id, accession_number)
    )""",
    """CREATE TABLE IF NOT EXISTS source_data.sec_company_facts (
        source TEXT NOT NULL,
        dataset TEXT NOT NULL,
        run_id TEXT NOT NULL,
        source_row_number BIGINT NOT NULL,
        cik TEXT NOT NULL,
        entity_name TEXT NOT NULL,
        taxonomy TEXT NOT NULL,
        concept TEXT NOT NULL,
        label TEXT NULL,
        description TEXT NULL,
        unit TEXT NOT NULL,
        value NUMERIC(76,30) NOT NULL,
        start_date DATE NULL,
        end_date DATE NOT NULL,
        accession_number TEXT NOT NULL,
        fiscal_year INTEGER NULL,
        fiscal_period TEXT NULL,
        form TEXT NOT NULL,
        filed_date DATE NOT NULL,
        frame TEXT NULL,
        CONSTRAINT pk_sec_company_facts
            PRIMARY KEY (source, dataset, run_id, source_row_number),
        CONSTRAINT fk_sec_company_facts_run
            FOREIGN KEY (source, dataset, run_id)
            REFERENCES source_data.ingestion_runs (source, dataset, run_id),
        CONSTRAINT ck_sec_company_facts_source_row_number
            CHECK (source_row_number >= 0),
        CONSTRAINT ck_sec_company_facts_identity
            CHECK (source = 'sec_edgar' AND dataset = 'company_facts'),
        CONSTRAINT ck_sec_company_facts_cik
            CHECK (cik ~ '^[0-9]{10}$'),
        CONSTRAINT ck_sec_company_facts_entity_name_nonblank
            CHECK (btrim(entity_name) <> ''),
        CONSTRAINT ck_sec_company_facts_taxonomy_nonblank
            CHECK (btrim(taxonomy) <> ''),
        CONSTRAINT ck_sec_company_facts_concept_nonblank
            CHECK (btrim(concept) <> ''),
        CONSTRAINT ck_sec_company_facts_unit_nonblank
            CHECK (btrim(unit) <> ''),
        CONSTRAINT ck_sec_company_facts_accession_nonblank
            CHECK (btrim(accession_number) <> ''),
        CONSTRAINT ck_sec_company_facts_form_nonblank
            CHECK (btrim(form) <> ''),
        CONSTRAINT ck_sec_company_facts_date_range
            CHECK (start_date IS NULL OR start_date <= end_date)
    )""",
    """CREATE TABLE IF NOT EXISTS source_data.fred_series_metadata (
        source TEXT NOT NULL,
        dataset TEXT NOT NULL,
        run_id TEXT NOT NULL,
        source_row_number BIGINT NOT NULL,
        series_id TEXT NOT NULL,
        realtime_start DATE NOT NULL,
        realtime_end DATE NOT NULL,
        title TEXT NOT NULL,
        observation_start DATE NOT NULL,
        observation_end DATE NOT NULL,
        frequency TEXT NOT NULL,
        frequency_short TEXT NOT NULL,
        units TEXT NOT NULL,
        units_short TEXT NOT NULL,
        seasonal_adjustment TEXT NOT NULL,
        seasonal_adjustment_short TEXT NOT NULL,
        last_updated TIMESTAMPTZ NOT NULL,
        popularity BIGINT NOT NULL,
        notes TEXT NULL,
        CONSTRAINT pk_fred_series_metadata
            PRIMARY KEY (source, dataset, run_id, source_row_number),
        CONSTRAINT fk_fred_series_metadata_run
            FOREIGN KEY (source, dataset, run_id)
            REFERENCES source_data.ingestion_runs (source, dataset, run_id),
        CONSTRAINT ck_fred_series_metadata_source_row_number
            CHECK (source_row_number >= 0),
        CONSTRAINT ck_fred_series_metadata_identity
            CHECK (source = 'fred' AND dataset = 'series_metadata'),
        CONSTRAINT ck_fred_series_metadata_series_id_nonblank
            CHECK (btrim(series_id) <> ''),
        CONSTRAINT ck_fred_series_metadata_title_nonblank
            CHECK (btrim(title) <> ''),
        CONSTRAINT ck_fred_series_metadata_frequency_nonblank
            CHECK (btrim(frequency) <> ''),
        CONSTRAINT ck_fred_series_metadata_frequency_short_nonblank
            CHECK (btrim(frequency_short) <> ''),
        CONSTRAINT ck_fred_series_metadata_units_nonblank
            CHECK (btrim(units) <> ''),
        CONSTRAINT ck_fred_series_metadata_units_short_nonblank
            CHECK (btrim(units_short) <> ''),
        CONSTRAINT ck_fred_series_metadata_seasonal_nonblank
            CHECK (btrim(seasonal_adjustment) <> ''),
        CONSTRAINT ck_fred_series_metadata_seasonal_short_nonblank
            CHECK (btrim(seasonal_adjustment_short) <> ''),
        CONSTRAINT ck_fred_series_metadata_realtime_range
            CHECK (realtime_start <= realtime_end),
        CONSTRAINT ck_fred_series_metadata_observation_range
            CHECK (observation_start <= observation_end),
        CONSTRAINT ck_fred_series_metadata_popularity
            CHECK (popularity >= 0),
        CONSTRAINT uq_fred_series_metadata_run_series
            UNIQUE (source, dataset, run_id, series_id)
    )""",
    """CREATE TABLE IF NOT EXISTS source_data.fred_series_observations (
        source TEXT NOT NULL,
        dataset TEXT NOT NULL,
        run_id TEXT NOT NULL,
        source_row_number BIGINT NOT NULL,
        series_id TEXT NOT NULL,
        realtime_start DATE NOT NULL,
        realtime_end DATE NOT NULL,
        observation_date DATE NOT NULL,
        value NUMERIC(76,30) NULL,
        CONSTRAINT pk_fred_series_observations
            PRIMARY KEY (source, dataset, run_id, source_row_number),
        CONSTRAINT fk_fred_series_observations_run
            FOREIGN KEY (source, dataset, run_id)
            REFERENCES source_data.ingestion_runs (source, dataset, run_id),
        CONSTRAINT ck_fred_series_observations_source_row_number
            CHECK (source_row_number >= 0),
        CONSTRAINT ck_fred_series_observations_identity
            CHECK (source = 'fred' AND dataset = 'series_observations'),
        CONSTRAINT ck_fred_series_observations_series_id_nonblank
            CHECK (btrim(series_id) <> ''),
        CONSTRAINT ck_fred_series_observations_realtime_range
            CHECK (realtime_start <= realtime_end),
        CONSTRAINT uq_fred_series_observations_run_grain
            UNIQUE (source, dataset, run_id, series_id, observation_date,
                realtime_start, realtime_end)
    )""",
)


def ensure_source_schema(connection: psycopg.Connection) -> None:
    """Execute source schema DDL inside the caller-owned transaction."""
    with connection.cursor() as cursor:
        for statement in SOURCE_SCHEMA_DDL:
            cursor.execute(statement)
