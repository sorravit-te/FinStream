with mapped_financial_facts as (
    select
        financial_facts.cik,
        financial_metric_mapping.metric_key,
        financial_facts.taxonomy,
        financial_facts.concept,
        financial_facts.unit,
        financial_facts.value,
        financial_facts.start_date,
        financial_facts.end_date,
        financial_facts.accession_number,
        financial_facts.fiscal_year,
        financial_facts.fiscal_period,
        financial_facts.form,
        financial_facts.filed_date,
        financial_facts.frame
    from {{ ref('stg_financial_facts') }} as financial_facts
    inner join {{ ref('financial_metric_mapping') }} as financial_metric_mapping
        on financial_facts.taxonomy = financial_metric_mapping.taxonomy
        and financial_facts.concept = financial_metric_mapping.concept
)

select
    cik,
    metric_key,
    taxonomy,
    concept,
    unit,
    value,
    start_date,
    end_date,
    accession_number,
    fiscal_year,
    fiscal_period,
    form,
    filed_date,
    frame
from mapped_financial_facts
