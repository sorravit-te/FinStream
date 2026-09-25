with expected_previous as (
    select
        company_key,
        metric_key,
        end_date,
        unit,
        current_value,
        lag(end_date) over (
            partition by company_key, metric_key, unit
            order by end_date asc
        ) as expected_previous_end_date,
        lag(current_value) over (
            partition by company_key, metric_key, unit
            order by end_date asc
        ) as expected_previous_value,
        previous_end_date as stored_previous_end_date,
        previous_value as stored_previous_value,
        absolute_change as stored_absolute_change,
        growth_rate as stored_growth_rate
    from {{ ref('mart_company_financial_growth') }}
),

violations as (
    -- 1. previous_end_date must match expected LAG(end_date)
    select
        company_key,
        metric_key,
        end_date,
        unit,
        'stored previous_end_date does not match expected lag(end_date)' as failure_reason
    from expected_previous
    where (stored_previous_end_date is distinct from expected_previous_end_date)

    union all

    -- 2. previous_value must match expected LAG(current_value)
    select
        company_key,
        metric_key,
        end_date,
        unit,
        'stored previous_value does not match expected lag(current_value)' as failure_reason
    from expected_previous
    where (stored_previous_value is distinct from expected_previous_value)

    union all

    -- 3. First observation: previous fields, absolute_change, and growth_rate must all be null
    select
        company_key,
        metric_key,
        end_date,
        unit,
        'first observation must have all previous and derived growth fields null' as failure_reason
    from expected_previous
    where expected_previous_end_date is null
      and (
          stored_previous_end_date is not null
          or stored_previous_value is not null
          or stored_absolute_change is not null
          or stored_growth_rate is not null
      )

    union all

    -- 4. Valid 350-380 day comparison: absolute_change must equal current_value - previous_value
    select
        company_key,
        metric_key,
        end_date,
        unit,
        'consecutive comparison has incorrect absolute_change' as failure_reason
    from expected_previous
    where expected_previous_end_date is not null
      and (end_date - expected_previous_end_date) between 350 and 380
      and (
          stored_absolute_change is null
          or stored_absolute_change != (current_value - expected_previous_value)
      )

    union all

    -- 5. Valid comparison with previous_value != 0: growth_rate must equal (current_value - previous_value) / ABS(previous_value)
    select
        company_key,
        metric_key,
        end_date,
        unit,
        'consecutive comparison with non-zero previous_value has incorrect growth_rate' as failure_reason
    from expected_previous
    where expected_previous_end_date is not null
      and (end_date - expected_previous_end_date) between 350 and 380
      and expected_previous_value != 0
      and (
          stored_growth_rate is null
          or stored_growth_rate != ((current_value - expected_previous_value) / abs(expected_previous_value))
      )

    union all

    -- 6. Valid comparison with previous_value = 0: absolute_change calculated, growth_rate must be null
    select
        company_key,
        metric_key,
        end_date,
        unit,
        'consecutive comparison with zero previous_value must have null growth_rate' as failure_reason
    from expected_previous
    where expected_previous_end_date is not null
      and (end_date - expected_previous_end_date) between 350 and 380
      and expected_previous_value = 0
      and (
          stored_absolute_change is null
          or stored_absolute_change != current_value
          or stored_growth_rate is not null
      )

    union all

    -- 7. Non-consecutive period: previous fields retained, but absolute_change and growth_rate must be null
    select
        company_key,
        metric_key,
        end_date,
        unit,
        'non-consecutive period must retain previous fields but have null absolute_change and growth_rate' as failure_reason
    from expected_previous
    where expected_previous_end_date is not null
      and (end_date - expected_previous_end_date) not between 350 and 380
      and (
          stored_previous_end_date != expected_previous_end_date
          or stored_previous_value != expected_previous_value
          or stored_absolute_change is not null
          or stored_growth_rate is not null
      )
)

select * from violations
