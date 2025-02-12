select
    DATE(created_at) as created_date
    , COUNT(*) as nb_creation
from {{ ref('core_tenant_account') }}
group by
    DATE(created_at)
