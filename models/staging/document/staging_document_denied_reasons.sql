with checked_options_details as (
    select
        id
        , comment as operator_comment
        , document_id
        , creation_date as created_at
        , document_category
        , document_sub_category
        , document_category_step
        , document_tenant_type
        , UNNEST(checked_options_id) as denied_option_id
        , UNNEST(checked_options) as denied_option_value
    from {{ source('dossierfacile', 'document_denied_reasons') }}
    {{ filter_recent_data('creation_date') }}
)

select
    CAST(id as INTEGER)
    , CAST(denied_option_id as INTEGER)
    , CAST({{ fix_encoding_character('denied_option_value') }} as VARCHAR) as denied_option_value
    , CAST(operator_comment as VARCHAR)
    , CAST(document_id as INTEGER)
    , CAST(created_at as TIMESTAMP) as document_denied_at
    , CAST(document_category as VARCHAR)
    , CAST(document_sub_category as VARCHAR)
    , CAST(document_category_step as VARCHAR)
    , CAST(document_tenant_type as VARCHAR)
from checked_options_details
