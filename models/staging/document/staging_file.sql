select
    CAST(id as INTEGER) as file_id
    , CAST(document_id as INTEGER) as document_id
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(number_of_pages as INTEGER) as page_number
from {{ source('dossierfacile', 'file') }}
{{ filter_recent_data('creation_date') }}
