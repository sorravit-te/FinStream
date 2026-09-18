select distinct end_date as date_day
from {{ ref('fact_financial_reported') }}
where end_date is not null

except

select date_day
from {{ ref('dim_date') }}
