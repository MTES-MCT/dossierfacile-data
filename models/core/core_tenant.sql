select *
from {{ source('production_copy', 'tenant') }}
