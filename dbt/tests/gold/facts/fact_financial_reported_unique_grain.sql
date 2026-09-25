select
    cik,
    taxonomy,
    concept,
    unit,
    start_date,
    end_date,
    accession_number,
    fiscal_year,
    fiscal_period,
    form,
    filed_date,
    frame
from {{ ref('fact_financial_reported') }}
group by
    cik,
    taxonomy,
    concept,
    unit,
    start_date,
    end_date,
    accession_number,
    fiscal_year,
    fiscal_period,
    form,
    filed_date,
    frame
having count(*) > 1
