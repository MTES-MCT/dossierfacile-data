select
    denied_option_id
    , denied_option_value
    , denied_option_sub_category
    , denied_option_code
    , document_category
    , document_sub_category
    , document_category_step
    , document_tenant_type
    , DATE(document_denied_at) as document_denied_date
    , COUNT(document_id) as nb_denied_documents
from {{ ref('core_document_denied_reasons') }}
group by
    DATE(document_denied_at)
    , denied_option_id
    , denied_option_value
    , denied_option_sub_category
    , denied_option_code
    , document_category
    , document_sub_category
    , document_category_step
    , document_tenant_type
