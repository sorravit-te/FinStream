with unverified_source_provenance as (
    select
        m.company_key,
        m.metric_key,
        m.end_date,
        m.unit,
        'selected fact does not match qualifying annual source occurrence in fact_financial_reported' as failure_reason
    from {{ ref('mart_company_financial_growth') }} as m
    left join {{ ref('fact_financial_reported') }} as f
        on f.cik = m.source_cik
        and f.accession_number = m.accession_number
        and f.metric_key = m.metric_key
        and f.end_date = m.end_date
        and f.unit = m.unit
        and f.form in ('10-K', '10-K/A')
        and f.fiscal_period = 'FY'
        and (
            (m.period_type = 'duration' and f.start_date is not null and (f.end_date - f.start_date) between 350 and 380)
            or (m.period_type = 'instant' and f.start_date is null)
        )
    where f.accession_number is null
),

invalid_mart_shape as (
    select
        company_key,
        metric_key,
        end_date,
        unit,
        'invalid form or period shape in mart columns' as failure_reason
    from {{ ref('mart_company_financial_growth') }}
    where form not in ('10-K', '10-K/A')
       or (period_type = 'duration' and (start_date is null or (end_date - start_date) not between 350 and 380))
       or (period_type = 'instant' and start_date is not null)
),

higher_ranked_candidate_exists as (
    select
        m.company_key,
        m.metric_key,
        m.end_date,
        m.unit,
        'lower-ranked filing selected over higher-ranked candidate' as failure_reason
    from {{ ref('mart_company_financial_growth') }} as m
    inner join {{ ref('fact_financial_reported') }} as f
        on f.metric_key = m.metric_key
        and f.end_date = m.end_date
        and f.unit = m.unit
    inner join {{ ref('company_identity_mapping') }} as c
        on f.cik = c.source_cik
        and c.company_key = m.company_key
    where f.form in ('10-K', '10-K/A')
      and f.fiscal_period = 'FY'
      and (
          (m.period_type = 'duration' and f.start_date is not null and (f.end_date - f.start_date) between 350 and 380)
          or (m.period_type = 'instant' and f.start_date is null)
      )
      and (
          f.filed_date > m.filed_date
          or (f.filed_date = m.filed_date and f.accession_number > m.accession_number)
      )
)

select * from unverified_source_provenance
union all
select * from invalid_mart_shape
union all
select * from higher_ranked_candidate_exists
