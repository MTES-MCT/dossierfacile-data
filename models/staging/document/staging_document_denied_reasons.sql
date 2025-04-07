with checked_options_details as (
    select
        id
        , comment as operator_comment
        , document_id
        , creation_date
        , UNNEST(checked_options_id) as denied_options_id
    from {{ source('dossierfacile', 'document_denied_reasons') }}
    {{ filter_recent_data('creation_date') }}
)

select
    CAST(id as INTEGER)
    , CAST(denied_options_id as INTEGER)
    , CAST(operator_comment as VARCHAR)
    , CAST(document_id as INTEGER)
    , CAST(creation_date as TIMESTAMP) as created_at
from checked_options_details
