with expected_grid as (
    select
        market.company_key,
        market.trading_date,
        series.series_id
    from {{ ref('mart_company_daily_performance') }} as market
    cross join {{ ref('dim_macro_series') }} as series
),

actual_grid as (
    select
        company_key,
        trading_date,
        series_id
    from {{ ref('mart_market_macro') }}
),

missing_rows as (
    select * from expected_grid
    except
    select * from actual_grid
),

unexpected_rows as (
    select * from actual_grid
    except
    select * from expected_grid
)

select
    company_key,
    trading_date,
    series_id,
    'missing expected market-series grid row' as failure_reason
from missing_rows

union all

select
    company_key,
    trading_date,
    series_id,
    'unexpected market-series grid row' as failure_reason
from unexpected_rows
