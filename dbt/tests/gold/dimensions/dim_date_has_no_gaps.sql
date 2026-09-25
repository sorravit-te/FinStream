with ordered_dates as (
    select
        date_day,
        lead(date_day) over (order by date_day) as next_date_day
    from {{ ref('dim_date') }}
)

select date_day
from ordered_dates
where next_date_day is not null
    and next_date_day <> date_day + 1
