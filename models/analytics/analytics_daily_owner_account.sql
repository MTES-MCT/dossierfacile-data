select
    DATE(created_at) as creation_date
    , COUNT(id) as nb_creation
    , SUM(nb_property_created) as nb_property_created
from {{ ref('core_owner_account') }}
group by
    DATE(created_at)
