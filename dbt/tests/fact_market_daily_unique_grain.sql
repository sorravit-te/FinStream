select
    symbol,
    trading_date
from {{ ref('fact_market_daily') }}
group by
    symbol,
    trading_date
having count(*) > 1
