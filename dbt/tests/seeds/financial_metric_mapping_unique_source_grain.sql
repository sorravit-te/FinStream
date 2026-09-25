select
    taxonomy,
    concept
from {{ ref('financial_metric_mapping') }}
group by
    taxonomy,
    concept
having count(*) > 1
