select
    series_id
from {{ ref('stg_macro_observations') }}
group by series_id
