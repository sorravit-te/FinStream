select
    company_key,
    trading_date,
    series_id,
    count(*) as row_count
from {{ ref('mart_market_macro') }}
group by
    company_key,
    trading_date,
    series_id
having count(*) > 1
