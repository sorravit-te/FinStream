select distinct observation_date as date_day
from {{ ref('fact_macro_observation') }}
where observation_date is not null

except

select date_day
from {{ ref('dim_date') }}
