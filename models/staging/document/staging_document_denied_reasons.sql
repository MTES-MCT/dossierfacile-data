with checked_options_details as (
    select
        id
        , UNNEST(checked_options) as checked_options
        , UNNEST(checked_options_id) as checked_options_id
    from {{ source('dossierfacile', 'document_denied_reasons') }}
    {{ filter_recent_data('creation_date') }}

)

select
    CAST(document_denied_reasons.id as INTEGER)
    , CAST(checked_options_details.checked_options as VARCHAR)
    , CAST(checked_options_details.checked_options_id as INTEGER)
    , CAST(document_denied_reasons.comment as VARCHAR)
    , CAST(document_denied_reasons.message_id as INTEGER)
    , CAST(document_denied_reasons.message_data as VARCHAR)
    , CAST(document_denied_reasons.document_id as INTEGER)
    , CAST(document_denied_reasons.creation_date as TIMESTAMP) as created_at
from {{ source('dossierfacile', 'document_denied_reasons') }}
inner join checked_options_details on document_denied_reasons.id = checked_options_details.id
{{ filter_recent_data('creation_date') }}
