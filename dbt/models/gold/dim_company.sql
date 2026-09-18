with ranked_companies as (
    select
        cik,
        entity_name,
        row_number() over (
            partition by cik
            order by
                filed_date desc,
                ingested_at desc,
                run_id desc,
                loaded_at desc,
                source_row_number desc
        ) as representative_name_rank
    from {{ ref('stg_financial_facts') }}
)

select
    cik,
    entity_name
from ranked_companies
where representative_name_rank = 1
