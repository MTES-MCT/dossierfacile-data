select
    CAST(id as INTEGER)
    , CAST(token as VARCHAR)
    , CAST(token_public as VARCHAR)
    , CAST(operator_date as TIMESTAMP)
    , CAST(application_type as VARCHAR)
    , CAST(dossier_pdf_document_status as VARCHAR)
    , CAST(last_update_date as TIMESTAMP) as last_update_at
    , CAST(pdf_dossier_file_id as INTEGER)
from apartment_sharing
{{ filter_recent_data('last_update_date') }}
