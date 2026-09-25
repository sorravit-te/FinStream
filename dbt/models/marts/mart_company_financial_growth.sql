with annual_candidates as (
    select
        c.company_key,
        c.current_ticker,
        f.cik as source_cik,
        f.metric_key,
        m.metric_name,
        m.period_type,
        f.taxonomy,
        f.concept,
        f.unit,
        f.fiscal_year,
        f.start_date,
        f.end_date,
        f.value as current_value,
        f.form,
        f.filed_date,
        f.accession_number
    from {{ ref('fact_financial_reported') }} as f
    inner join {{ ref('company_identity_mapping') }} as c
        on f.cik = c.source_cik
    inner join {{ ref('dim_financial_metric') }} as m
        on f.metric_key = m.metric_key
    where f.form in ('10-K', '10-K/A')
      and f.fiscal_period = 'FY'
      and (
          (
              m.period_type = 'duration'
              and f.start_date is not null
              and f.end_date is not null
              and (f.end_date - f.start_date) between 350 and 380
          )
          or (
              m.period_type = 'instant'
              and f.start_date is null
              and f.end_date is not null
          )
      )
),

ranked_representative_facts as (
    select
        company_key,
        current_ticker,
        source_cik,
        metric_key,
        metric_name,
        period_type,
        taxonomy,
        concept,
        unit,
        fiscal_year,
        start_date,
        end_date,
        current_value,
        form,
        filed_date,
        accession_number,
        row_number() over (
            partition by company_key, metric_key, end_date, unit
            order by filed_date desc, accession_number desc
        ) as fact_rank
    from annual_candidates
),

representative_facts as (
    select
        company_key,
        current_ticker,
        source_cik,
        metric_key,
        metric_name,
        period_type,
        taxonomy,
        concept,
        unit,
        fiscal_year,
        start_date,
        end_date,
        current_value,
        form,
        filed_date,
        accession_number
    from ranked_representative_facts
    where fact_rank = 1
),

with_previous_period as (
    select
        company_key,
        current_ticker,
        source_cik,
        metric_key,
        metric_name,
        period_type,
        taxonomy,
        concept,
        unit,
        fiscal_year,
        start_date,
        end_date,
        current_value,
        form,
        filed_date,
        accession_number,
        lag(end_date) over (
            partition by company_key, metric_key, unit
            order by end_date asc
        ) as previous_end_date,
        lag(current_value) over (
            partition by company_key, metric_key, unit
            order by end_date asc
        ) as previous_value
    from representative_facts
),

final as (
    select
        company_key,
        current_ticker,
        source_cik,
        metric_key,
        metric_name,
        period_type,
        taxonomy,
        concept,
        unit,
        fiscal_year,
        start_date,
        end_date,
        current_value,
        previous_end_date,
        previous_value,
        case
            when previous_end_date is not null
                 and (end_date - previous_end_date) between 350 and 380
            then current_value - previous_value
            else null
        end as absolute_change,
        case
            when previous_end_date is not null
                 and (end_date - previous_end_date) between 350 and 380
                 and previous_value != 0
            then (current_value - previous_value) / abs(previous_value)
            else null
        end as growth_rate,
        form,
        filed_date,
        accession_number
    from with_previous_period
)

select
    company_key,
    current_ticker,
    source_cik,
    metric_key,
    metric_name,
    period_type,
    taxonomy,
    concept,
    unit,
    fiscal_year,
    start_date,
    end_date,
    current_value,
    previous_end_date,
    previous_value,
    absolute_change,
    growth_rate,
    form,
    filed_date,
    accession_number
from final
