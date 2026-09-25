select
    metric_key
from {{ ref('financial_metric_mapping') }}
group by metric_key
having count(distinct metric_name) > 1
