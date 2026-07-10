{{ config(
    materialized = 'incremental', 
    unique_key = 'id',
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['tenant_id']},
      {'columns': ['guarantor_id']},
      {'columns': ['created_at'], 'type': 'brin'} 
    ]
) }}

{#
  On choisit incremental pour éviter de recalculer l'index pour les millions
  de lignes à chaque run de la table. On ne recalcule que pour les nouvelles
  valeurs en se basant sur l'attribut created_at.
  BRIN sur created_at : index ultra léger car utilise l'ordre chronologique
  d'écriture (les dernières lignes = les plus récentes).
#}

with casting_log as (
    select
        CAST(id as INTEGER)
        , CAST(tenant_id as INTEGER)
        , CAST(log_details ->> 'guarantorId' as INTEGER) as guarantor_id
        , CAST(operator_id as INTEGER)
        , CAST(log_type as VARCHAR)
        , CAST(creation_date as TIMESTAMP) as created_at
        -- flag migrate dans tenant_log pour indiquer les lignes qui ont été migrées 
        -- utile si on veut faire qq chose de plus fin comme par exemple dupliquer le dernier logs de suppression en un log d’archivage et de suppression
        -- il y avait un bug précédemment qui ne permettait pas de distinguer la suppression quand un archivage avait eu lieu.
        , CAST(migrate as BOOLEAN)
        -- When the log type is ACCOUNT_EDITED and the log details are not null, we extract the edition type, document category and sub category from the log details
        , log_details ->> 'editionType' || '_DOCUMENT' as edition_type
        , CAST(log_details ->> 'documentId' as INTEGER) as document_id
        , log_details ->> 'documentCategory' as document_category
        , log_details ->> 'documentSubCategory' as document_sub_category
        -- When the log type is OPERATOR_COMMENT, we extract the operator comment from the log details
        , log_details ->> 'comment' as operator_comment
    from {{ source('dossierfacile', 'tenant_log') }}
    {{ filter_recent_data('creation_date') }}

    {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) - INTERVAL '2 day' FROM {{ this }}) 
    {% endif %}
)

{#
  Indique de calculer les index uniquement pour les "nouvelles valeurs", 
  celles qui sont plus récentes que la dernière valeur created_at stockée dans la table
  On rajoute une sécurité de 2 jours en cas de mauvaise manip/bug au niveau des batch 
  de la réplication quotidienne (pourrait être passé à 1 jour)
#}

select
    casting_log.id
    , casting_log.tenant_id
    , casting_log.guarantor_id
    , casting_log.operator_id
    , casting_log.log_type
    , casting_log.created_at
    , casting_log.migrate
    , casting_log.edition_type
    , casting_log.document_id
    , casting_log.document_category
    , casting_log.document_sub_category
    , casting_log.operator_comment
    , case when casting_log.guarantor_id is not null then 'GUARANTOR' else 'TENANT' end as tenant_type
from casting_log
