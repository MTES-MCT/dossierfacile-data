select
    CAST(id as INTEGER) 
    , CAST(property_id as INTEGER) 
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(log_type as VARCHAR) 
    , CAST(apartment_sharing_id as INTEGER)
from {{ source('dossierfacile', 'property_log') }}
{{ filter_recent_data('creation_date') }}
