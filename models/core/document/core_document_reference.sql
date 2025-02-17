select distinct
    CAST(document_category as VARCHAR)
    , CAST(document_sub_category as VARCHAR)
from {{ ref('staging_document') }}
