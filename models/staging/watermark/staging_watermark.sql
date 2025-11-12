-- FiligraneFacile

select
    CAST(id as INTEGER)
    , CAST(created_date as TIMESTAMP) as created_at
    , CAST(token as VARCHAR)
    , CAST(pdf_status as VARCHAR)
    , CAST(pdf_file_id as INTEGER)
from {{ source('dossierfacile', 'watermark_document') }}
{{ filter_recent_data('created_date') }}
