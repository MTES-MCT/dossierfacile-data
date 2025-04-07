select
    denied_options_id
    , denied_options_value
    , denied_options_sub_category
    , denied_options_code
    , document_category
    , document_sub_category
    , document_category_step
    , document_tenant_type
    , DATE(document_denied_at) as document_denied_date
    , COUNT(document_id) as nb_denied_documents
from {{ ref('core_document_denied_reasons') }}
group by
    DATE(document_denied_at)
    , denied_options_id
    , denied_options_value
    , denied_options_sub_category
    , denied_options_code
    , document_category
    , document_sub_category
    , document_category_step
    , document_tenant_type
