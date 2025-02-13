select
    DATE(created_at) as created_at
    , COUNT(id) as nb_creation
from {{ ref('core_owner_account') }}
group by
    DATE(created_at)
