select
    company_key,
    trading_date
from {{ ref('mart_company_daily_performance') }}
group by
    company_key,
    trading_date
having count(*) > 1
