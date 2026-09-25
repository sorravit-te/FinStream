select
    company_key,
    metric_key,
    end_date,
    unit,
    count(*) as row_count
from {{ ref('mart_company_financial_growth') }}
group by
    company_key,
    metric_key,
    end_date,
    unit
having count(*) > 1
