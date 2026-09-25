select
    metric_key,
    metric_name,
    period_type
from {{ ref('financial_metric_mapping') }}
group by
    metric_key,
    metric_name,
    period_type
