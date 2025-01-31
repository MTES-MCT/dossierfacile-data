select
    DATE(creation_date) as creation_date
    , COUNT(*) as nb_creation
from {{ ref('core_tenant_account') }}
group by
    DATE(creation_date)
