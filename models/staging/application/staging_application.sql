select
    CAST(id as INTEGER) as id
    , CAST(token as VARCHAR) as token
    , CAST(token_public as VARCHAR) as token_public
    , CAST(application_type as VARCHAR) as application_type
    , CAST(last_update_date as TIMESTAMP) as updated_at
    , CAST(dossier_pdf_document_status as VARCHAR) as application_pdf_status
    , CAST(pdf_dossier_file_id as INTEGER) as application_pdf_file_id
from {{ source('dossierfacile', 'apartment_sharing') }}
{{ filter_recent_data('last_update_date') }}
