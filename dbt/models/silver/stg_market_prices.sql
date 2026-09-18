with market_prices_with_ingestion as (
    select
        market_prices.symbol,
        market_prices.trading_date,
        market_prices.open,
        market_prices.high,
        market_prices.low,
        market_prices.close,
        market_prices.volume,
        market_prices.source,
        market_prices.dataset,
        market_prices.run_id,
        market_prices.source_row_number,
        ingestion_runs.ingested_at,
        ingestion_runs.loaded_at
    from {{ source('source_data', 'market_daily_prices') }} as market_prices
    inner join {{ source('source_data', 'ingestion_runs') }} as ingestion_runs
        on market_prices.source = ingestion_runs.source
        and market_prices.dataset = ingestion_runs.dataset
        and market_prices.run_id = ingestion_runs.run_id
),

ranked_market_prices as (
    select
        symbol,
        trading_date,
        open,
        high,
        low,
        close,
        volume,
        source,
        dataset,
        run_id,
        source_row_number,
        ingested_at,
        loaded_at,
        row_number() over (
            partition by symbol, trading_date
            order by
                ingested_at desc,
                run_id desc,
                loaded_at desc,
                source_row_number desc
        ) as ingestion_recency_rank
    from market_prices_with_ingestion
)

select
    symbol,
    trading_date,
    open,
    high,
    low,
    close,
    volume,
    source,
    dataset,
    run_id,
    source_row_number,
    ingested_at,
    loaded_at
from ranked_market_prices
where ingestion_recency_rank = 1
