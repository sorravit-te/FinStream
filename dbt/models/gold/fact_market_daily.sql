select
    symbol,
    trading_date,
    open,
    high,
    low,
    close,
    volume
from {{ ref('stg_market_prices') }}
