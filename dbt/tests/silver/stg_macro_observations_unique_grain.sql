select
    series_id,
    observation_date,
    realtime_start,
    realtime_end
from {{ ref('stg_macro_observations') }}
group by
    series_id,
    observation_date,
    realtime_start,
    realtime_end
having count(*) > 1
