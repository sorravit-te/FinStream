select
    series_id,
    observation_date
from {{ ref('fact_macro_observation') }}
group by
    series_id,
    observation_date
having count(*) > 1
