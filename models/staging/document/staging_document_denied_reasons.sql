with checked_options_details as (
    select
        id
        , comment
        , message_id
        , message_data
        , document_id
        , creation_date
        , UNNEST(checked_options) as checked_options
        , UNNEST(checked_options_id) as checked_options_id
    from {{ source('dossierfacile', 'document_denied_reasons') }}
    {{ filter_recent_data('creation_date') }}

)

select
    CAST(id as INTEGER)
    , CAST(checked_options as VARCHAR)
    , CAST(comment as VARCHAR)
    , CAST(message_id as INTEGER)
    , CAST(checked_options_id as INTEGER)
    , CAST(message_data as VARCHAR)
    , CAST(document_id as INTEGER)
    , CAST(creation_date as TIMESTAMP) as created_at
from checked_options_details
