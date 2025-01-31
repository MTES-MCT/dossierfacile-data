select
    DATE(creation_date) as creation_date
    , COUNT(*)
from {{ ref('core_tenant_account') }}
group by
    DATE(creation_date)
