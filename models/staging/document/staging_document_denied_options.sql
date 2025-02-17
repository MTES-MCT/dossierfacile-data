select
    CAST(id as INTEGER)
    , CAST(message_value as VARCHAR)
    , CAST(document_sub_category as VARCHAR)
    , CAST(document_user_type as VARCHAR)
    , CAST(code as VARCHAR)
from {{ source('dossierfacile', 'document_denied_options') }}
