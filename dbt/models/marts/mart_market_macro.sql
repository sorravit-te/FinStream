with market_series_grid as (
    select
        market.company_key,
        market.current_ticker,
        market.current_cik,
        market.company_name,
        market.trading_date,
        series.series_id,
        market.close as market_close,
        market.volume as market_volume
    from {{ ref('mart_company_daily_performance') }} as market
    cross join {{ ref('dim_macro_series') }} as series
),

aligned_macro_observations as (
    select
        market_series_grid.company_key,
        market_series_grid.current_ticker,
        market_series_grid.current_cik,
        market_series_grid.company_name,
        market_series_grid.trading_date,
        market_series_grid.series_id,
        market_series_grid.market_close,
        market_series_grid.market_volume,
        macro_observation.observation_date as macro_observation_date,
        macro_observation.value as macro_value
    from market_series_grid
    left join lateral (
        select
            observation_date,
            value
        from {{ ref('fact_macro_observation') }} as macro
        where macro.series_id = market_series_grid.series_id
          and macro.observation_date <= market_series_grid.trading_date
        order by macro.observation_date desc
        limit 1
    ) as macro_observation on true
),

final as (
    select
        company_key,
        current_ticker,
        current_cik,
        company_name,
        trading_date,
        series_id,
        market_close,
        market_volume,
        macro_observation_date,
        macro_value,
        case
            when macro_observation_date is not null
                then trading_date - macro_observation_date
            else null
        end as macro_age_days
    from aligned_macro_observations
)

select
    company_key,
    current_ticker,
    current_cik,
    company_name,
    trading_date,
    series_id,
    market_close,
    market_volume,
    macro_observation_date,
    macro_value,
    macro_age_days
from final
