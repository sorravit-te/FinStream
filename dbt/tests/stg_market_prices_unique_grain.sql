select
    symbol,
    trading_date
from {{ ref('stg_market_prices') }}
group by
    symbol,
    trading_date
having count(*) > 1
