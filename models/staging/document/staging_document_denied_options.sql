select
    CAST(id as INTEGER)
    , CAST(message_value as VARCHAR) as denied_option_value
    , CAST(document_sub_category as VARCHAR)
    , UPPER(CAST(document_user_type as VARCHAR)) as document_tenant_type
    , CAST(code as VARCHAR)
from {{ source('dossierfacile', 'document_denied_options') }}
