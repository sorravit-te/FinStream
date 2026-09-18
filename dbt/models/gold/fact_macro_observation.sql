with ranked_macro_observations as (
    select
        series_id,
        observation_date,
        realtime_start,
        realtime_end,
        value,
        row_number() over (
            partition by series_id, observation_date
            order by
                realtime_start desc,
                realtime_end desc,
                ingested_at desc,
                run_id desc,
                loaded_at desc,
                source_row_number desc
        ) as current_observation_rank
    from {{ ref('stg_macro_observations') }}
)

select
    series_id,
    observation_date,
    realtime_start,
    realtime_end,
    value
from ranked_macro_observations
where current_observation_rank = 1
