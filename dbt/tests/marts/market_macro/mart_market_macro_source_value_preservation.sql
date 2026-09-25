select
    mart.company_key,
    mart.trading_date,
    mart.series_id,
    'latest eligible source date or value was not preserved' as failure_reason
from {{ ref('mart_market_macro') }} as mart
left join lateral (
    select
        macro.observation_date,
        macro.value
    from {{ ref('fact_macro_observation') }} as macro
    where macro.series_id = mart.series_id
      and macro.observation_date <= mart.trading_date
    order by macro.observation_date desc
    limit 1
) as expected_macro on true
where mart.macro_observation_date is distinct from expected_macro.observation_date
   or mart.macro_value is distinct from expected_macro.value
