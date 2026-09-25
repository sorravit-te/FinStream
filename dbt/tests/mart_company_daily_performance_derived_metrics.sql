with ranked_rows as (
    select
        company_key,
        trading_date,
        close,
        previous_close,
        daily_change,
        daily_return,
        row_number() over (
            partition by company_key
            order by trading_date asc
        ) as row_num,
        lag(close) over (
            partition by company_key
            order by trading_date asc
        ) as expected_previous_close
    from {{ ref('mart_company_daily_performance') }}
)

select
    company_key,
    trading_date,
    row_num,
    close,
    previous_close,
    expected_previous_close,
    daily_change,
    daily_return
from ranked_rows
where
    -- First observation per company must have null derived metrics
    (row_num = 1 and (
        previous_close is not null
        or daily_change is not null
        or daily_return is not null
    ))
    -- Later rows must have previous_close matching preceding market close
    or (row_num > 1 and (
        previous_close is distinct from expected_previous_close
    ))
    -- daily_change must equal close - previous_close when previous_close is present
    or (previous_close is not null and (
        daily_change is null
        or abs(daily_change - (close - previous_close)) > 0.000001
    ))
    -- daily_return must equal (close / previous_close) - 1 when previous_close != 0
    or (previous_close is not null and previous_close != 0 and (
        daily_return is null
        or abs(daily_return - ((close / previous_close) - 1.0)) > 0.000001
    ))
    -- If previous_close is 0 or null, daily_return must be null
    or ((previous_close is null or previous_close = 0) and daily_return is not null)
