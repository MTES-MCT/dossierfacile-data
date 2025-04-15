select
    denied_option_id
    , denied_option_value
    , denied_option_code
    , document_category
    , document_sub_category
    , document_tenant_type
    , DATE(document_denied_at) as denial_date
    , COUNT(unique_id) as nb_denial_reasons
from {{ ref('core_document_denied_reasons') }}
group by
    DATE(document_denied_at)
    , denied_option_id
    , denied_option_value
    , denied_option_code
    , document_category
    , document_sub_category
    , document_tenant_type
