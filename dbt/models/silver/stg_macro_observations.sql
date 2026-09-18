with observations_with_ingestion as (
    select
        observations.series_id,
        observations.observation_date,
        observations.realtime_start,
        observations.realtime_end,
        observations.value,
        observations.source,
        observations.dataset,
        observations.run_id,
        observations.source_row_number,
        ingestion_runs.ingested_at,
        ingestion_runs.loaded_at
    from {{ source('source_data', 'fred_series_observations') }} as observations
    inner join {{ source('source_data', 'ingestion_runs') }} as ingestion_runs
        on observations.source = ingestion_runs.source
        and observations.dataset = ingestion_runs.dataset
        and observations.run_id = ingestion_runs.run_id
),

ranked_observations as (
    select
        series_id,
        observation_date,
        realtime_start,
        realtime_end,
        value,
        source,
        dataset,
        run_id,
        source_row_number,
        ingested_at,
        loaded_at,
        row_number() over (
            partition by
                series_id,
                observation_date,
                realtime_start,
                realtime_end
            order by
                ingested_at desc,
                run_id desc,
                loaded_at desc,
                source_row_number desc
        ) as ingestion_recency_rank
    from observations_with_ingestion
)

select
    series_id,
    observation_date,
    realtime_start,
    realtime_end,
    value,
    source,
    dataset,
    run_id,
    source_row_number,
    ingested_at,
    loaded_at
from ranked_observations
where ingestion_recency_rank = 1
