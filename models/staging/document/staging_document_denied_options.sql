select
    CAST(id as INTEGER)
    , CAST(message_value as VARCHAR) as denied_options_value
    , CAST(document_sub_category as VARCHAR)
    , CAST(document_user_type as VARCHAR) as document_tenant_type
    , CAST(code as VARCHAR)
from {{ source('dossierfacile', 'document_denied_options') }}
