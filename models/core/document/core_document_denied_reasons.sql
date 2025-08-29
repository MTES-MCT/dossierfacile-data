select
    {{ dbt_utils.generate_surrogate_key(['id', 'denied_option_id']) }} as unique_id
    , document_denied_at
    , document_id
    , denied_option_id
    , denied_option_value

    , document_category
    , document_sub_category
    , document_category_step
    , document_tenant_type

    , operator_comment
from {{ ref('staging_document_denied_reasons') }}
