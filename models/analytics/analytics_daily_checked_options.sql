select
    document_category
    , document_sub_category
    , checked_options
    , document_user_type
    , tenant_origin
    , DATE(document_denied_at) as document_denied_at
    , COUNT(*) as nb_checked_options
from {{ ref('core_document_denied') }}
group by
    DATE(document_denied_at)
    , document_category
    , document_sub_category
    , checked_options
    , document_user_type
    , tenant_origin
