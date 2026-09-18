with silver_dates as (
    select trading_date as date_value
    from {{ ref('stg_market_prices') }}

    union all

    select start_date as date_value
    from {{ ref('stg_financial_facts') }}

    union all

    select end_date as date_value
    from {{ ref('stg_financial_facts') }}

    union all

    select filed_date as date_value
    from {{ ref('stg_financial_facts') }}

    union all

    select observation_date as date_value
    from {{ ref('stg_macro_observations') }}
),

date_bounds as (
    select
        min(date_value) as min_date,
        max(date_value) as max_date
    from silver_dates
),

date_spine as (
    select generated_date::date as date_day
    from date_bounds
    cross join lateral generate_series(
        min_date,
        max_date,
        interval '1 day'
    ) as generated_date
)

select
    date_day,
    extract(year from date_day)::integer as year,
    extract(quarter from date_day)::integer as quarter,
    extract(month from date_day)::integer as month,
    extract(day from date_day)::integer as day_of_month,
    extract(isodow from date_day)::integer as day_of_week,
    (extract(isodow from date_day) in (6, 7)) as is_weekend
from date_spine
