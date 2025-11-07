-- FiligraneFacile

select
    id
    , created_at
    , pdf_status
from {{ ref('staging_watermark') }}
