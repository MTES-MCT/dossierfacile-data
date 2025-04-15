-- Ce modèle a été créé afin d'avoir une liste de toutes les catégories et sous-catégories de documents utilisées
-- Il s'agit d'un workaround pour pallier à la non-normalisation des données dans la table staging_document_denied_options
-- En effet, seule la colonne document_sub_category est disponible dans la table staging_document_denied_options
-- Il mqanque l'information sur la catégorie du document

select distinct
    document_category
    , document_sub_category
from {{ ref('staging_document') }}
order by
    document_category
    , document_sub_category
