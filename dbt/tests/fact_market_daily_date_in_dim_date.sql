select distinct trading_date as date_day
from {{ ref('fact_market_daily') }}
where trading_date is not null

except

select date_day
from {{ ref('dim_date') }}
