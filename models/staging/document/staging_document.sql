-- à mettre à jour avec les vraies colonnes de la table document
select
    CAST(id as INTEGER)
    , CAST(tenant_id as INTEGER)
    , CAST(guarantor_id as INTEGER)
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(document_category as VARCHAR)
    , CAST(document_sub_category as VARCHAR)
from {{ source('dossierfacile', 'document') }}
{{ filter_recent_data('creation_date') }}
