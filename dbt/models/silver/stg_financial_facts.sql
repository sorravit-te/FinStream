with financial_facts_with_ingestion as (
    select
        financial_facts.cik,
        financial_facts.entity_name,
        financial_facts.taxonomy,
        financial_facts.concept,
        financial_facts.label,
        financial_facts.description,
        financial_facts.unit,
        financial_facts.value,
        financial_facts.start_date,
        financial_facts.end_date,
        financial_facts.accession_number,
        financial_facts.fiscal_year,
        financial_facts.fiscal_period,
        financial_facts.form,
        financial_facts.filed_date,
        financial_facts.frame,
        financial_facts.source,
        financial_facts.dataset,
        financial_facts.run_id,
        financial_facts.source_row_number,
        ingestion_runs.ingested_at,
        ingestion_runs.loaded_at
    from {{ source('source_data', 'sec_company_facts') }} as financial_facts
    inner join {{ source('source_data', 'ingestion_runs') }} as ingestion_runs
        on financial_facts.source = ingestion_runs.source
        and financial_facts.dataset = ingestion_runs.dataset
        and financial_facts.run_id = ingestion_runs.run_id
),

ranked_financial_facts as (
    select
        cik,
        entity_name,
        taxonomy,
        concept,
        label,
        description,
        unit,
        value,
        start_date,
        end_date,
        accession_number,
        fiscal_year,
        fiscal_period,
        form,
        filed_date,
        frame,
        source,
        dataset,
        run_id,
        source_row_number,
        ingested_at,
        loaded_at,
        row_number() over (
            partition by
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
            order by
                ingested_at desc,
                run_id desc,
                loaded_at desc,
                source_row_number desc
        ) as ingestion_recency_rank
    from financial_facts_with_ingestion
)

select
    cik,
    entity_name,
    taxonomy,
    concept,
    label,
    description,
    unit,
    value,
    start_date,
    end_date,
    accession_number,
    fiscal_year,
    fiscal_period,
    form,
    filed_date,
    frame,
    source,
    dataset,
    run_id,
    source_row_number,
    ingested_at,
    loaded_at
from ranked_financial_facts
where ingestion_recency_rank = 1
