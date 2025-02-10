select
    CAST(id as INTEGER) as id
    , CAST(property_id as INTEGER) as property_id
    , CAST(creation_date as TIMESTAMP) as creation_date
    , CAST(log_type as VARCHAR) as log_type
    , CAST(apartment_sharing_id as INTEGER) as apartment_sharing_id
from {{ source('dossierfacile', 'property_log') }}
{{ filter_recent_data('creation_date') }}
