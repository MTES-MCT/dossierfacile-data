select distinct
    document_category
    , document_sub_category
from {{ ref('staging_document') }}
