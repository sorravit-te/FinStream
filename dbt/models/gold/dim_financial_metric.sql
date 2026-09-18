select
    metric_key,
    metric_name
from {{ ref('financial_metric_mapping') }}
group by
    metric_key,
    metric_name
