with current_companies as (
    select
        company_key,
        current_ticker,
        source_cik as current_cik
    from {{ ref('company_identity_mapping') }}
    where is_current_registrant = true
),

market_observations as (
    select
        c.company_key,
        c.current_ticker,
        c.current_cik,
        d.entity_name as company_name,
        m.trading_date,
        m.open,
        m.high,
        m.low,
        m.close,
        m.volume
    from {{ ref('fact_market_daily') }} as m
    inner join current_companies as c
        on m.symbol = c.current_ticker
    inner join {{ ref('dim_company') }} as d
        on c.current_cik = d.cik
),

with_previous_close as (
    select
        company_key,
        current_ticker,
        current_cik,
        company_name,
        trading_date,
        open,
        high,
        low,
        close,
        volume,
        lag(close) over (
            partition by company_key
            order by trading_date asc
        ) as previous_close
    from market_observations
),

final as (
    select
        company_key,
        current_ticker,
        current_cik,
        company_name,
        trading_date,
        open,
        high,
        low,
        close,
        volume,
        previous_close,
        case
            when previous_close is not null then close - previous_close
            else null
        end as daily_change,
        case
            when previous_close is not null and previous_close != 0
                then (close / previous_close) - 1.0
            else null
        end as daily_return
    from with_previous_close
)

select
    company_key,
    current_ticker,
    current_cik,
    company_name,
    trading_date,
    open,
    high,
    low,
    close,
    volume,
    previous_close,
    daily_change,
    daily_return
from final
