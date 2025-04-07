select
    id
    , document_category
    , document_sub_category
    , checked_options_id
    , checked_options
    , document_user_type
    , tenant_origin
    , DATE(document_denied_at) as document_denied_at
    , COUNT(distinct checked_options_id) as nb_checked_options
from {{ ref('core_document_denied_reasons') }}
group by
    DATE(document_denied_at)
    , id
    , document_category
    , document_sub_category
    , checked_options_id
    , checked_options
    , document_user_type
    , tenant_origin
