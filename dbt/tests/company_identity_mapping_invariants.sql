with current_per_company as (
    select
        company_key,
        count(case when is_current_registrant = true then 1 end) as current_count
    from {{ ref('company_identity_mapping') }}
    group by company_key
),

current_per_ticker as (
    select
        current_ticker,
        count(distinct company_key) as company_count
    from {{ ref('company_identity_mapping') }}
    where is_current_registrant = true
    group by current_ticker
),

company_per_cik as (
    select
        source_cik,
        count(distinct company_key) as company_count
    from {{ ref('company_identity_mapping') }}
    group by source_cik
),

xom_predecessor_count as (
    select
        count(*) as row_count
    from {{ ref('company_identity_mapping') }}
    where company_key = 'XOM'
      and source_cik = '0000034088'
      and registrant_role = 'predecessor'
      and is_current_registrant = false
),

xom_successor_count as (
    select
        count(*) as row_count
    from {{ ref('company_identity_mapping') }}
    where company_key = 'XOM'
      and source_cik = '0002115436'
      and registrant_role = 'successor'
      and is_current_registrant = true
)

-- Invariant 1: every company_key has exactly one row where is_current_registrant = true
select
    company_key as entity_id,
    'company_key must have exactly one current registrant' as failure_reason
from current_per_company
where current_count != 1

union all

-- Invariant 2: every current_ticker maps to exactly one current company_key
select
    current_ticker as entity_id,
    'current_ticker maps to multiple current company_keys' as failure_reason
from current_per_ticker
where company_count != 1

union all

-- Invariant 3: a source_cik belongs to exactly one analytical company
select
    source_cik as entity_id,
    'source_cik belongs to multiple company_keys' as failure_reason
from company_per_cik
where company_count != 1

union all

-- Invariant 4: XOM predecessor must exist as exactly one non-current predecessor registrant
select
    'XOM' as entity_id,
    'XOM predecessor (CIK 0000034088, role predecessor, non-current) must exist exactly once' as failure_reason
from xom_predecessor_count
where row_count != 1

union all

-- Invariant 5: XOM successor must exist as exactly one current successor registrant
select
    'XOM' as entity_id,
    'XOM successor (CIK 0002115436, role successor, current) must exist exactly once' as failure_reason
from xom_successor_count
where row_count != 1
