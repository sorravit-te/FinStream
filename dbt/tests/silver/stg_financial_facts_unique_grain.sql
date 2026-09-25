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
from {{ ref('stg_financial_facts') }}
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
