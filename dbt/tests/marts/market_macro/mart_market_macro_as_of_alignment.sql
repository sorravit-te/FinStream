with expected_alignment as (
    select
        mart.company_key,
        mart.trading_date,
        mart.series_id,
        mart.macro_observation_date,
        mart.macro_value,
        mart.macro_age_days,
        max(macro.observation_date) as expected_macro_observation_date
    from {{ ref('mart_market_macro') }} as mart
    left join {{ ref('fact_macro_observation') }} as macro
        on macro.series_id = mart.series_id
        and macro.observation_date <= mart.trading_date
    group by
        mart.company_key,
        mart.trading_date,
        mart.series_id,
        mart.macro_observation_date,
        mart.macro_value,
        mart.macro_age_days
),

violations as (
    select
        aligned.company_key,
        aligned.trading_date,
        aligned.series_id,
        'stored observation date is not the latest eligible reference date' as failure_reason
    from expected_alignment as aligned
    where aligned.macro_observation_date is distinct from aligned.expected_macro_observation_date

    union all

    select
        aligned.company_key,
        aligned.trading_date,
        aligned.series_id,
        'stored observation date occurs after the trading date' as failure_reason
    from expected_alignment as aligned
    where aligned.macro_observation_date > aligned.trading_date

    union all

    select
        aligned.company_key,
        aligned.trading_date,
        aligned.series_id,
        'a later eligible macro observation exists' as failure_reason
    from expected_alignment as aligned
    where exists (
        select 1
        from {{ ref('fact_macro_observation') }} as later_macro
        where later_macro.series_id = aligned.series_id
          and later_macro.observation_date <= aligned.trading_date
          and later_macro.observation_date > aligned.macro_observation_date
    )

    union all

    select
        aligned.company_key,
        aligned.trading_date,
        aligned.series_id,
        'macro age is not the exact non-negative reference-date age' as failure_reason
    from expected_alignment as aligned
    where aligned.macro_observation_date is not null
      and (
          aligned.macro_age_days is distinct from (
              aligned.trading_date - aligned.macro_observation_date
          )
          or aligned.macro_age_days < 0
      )

    union all

    select
        aligned.company_key,
        aligned.trading_date,
        aligned.series_id,
        'rows without an eligible observation must have all aligned fields null' as failure_reason
    from expected_alignment as aligned
    where aligned.expected_macro_observation_date is null
      and (
          aligned.macro_observation_date is not null
          or aligned.macro_value is not null
          or aligned.macro_age_days is not null
      )
)

select * from violations
