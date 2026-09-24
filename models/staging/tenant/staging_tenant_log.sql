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

with casting_log as (
    select
        CAST(id as INTEGER)
        , CAST(tenant_id as INTEGER)
        , CAST(log_details ->> 'guarantorId' as INTEGER) as guarantor_id
        , CAST(operator_id as INTEGER)
        , CAST(log_type as VARCHAR)
        , CAST(creation_date as TIMESTAMP) as created_at
        -- When the log type is ACCOUNT_EDITED and the log details are not null, we extract the edition type, document category and sub category from the log details
        , log_details ->> 'editionType' || '_DOCUMENT' as edition_type
        , CAST(log_details ->> 'documentId' as INTEGER) as document_id
        , log_details ->> 'documentCategory' as document_category
        , log_details ->> 'documentSubCategory' as document_sub_category
        -- When the log type is OPERATOR_COMMENT, we extract the operator comment from the log details
        , log_details ->> 'comment' as operator_comment
    from {{ source('dossierfacile', 'tenant_log') }}

    {% if is_incremental() %}
        where creation_date > (select MAX(created_at) - INTERVAL '2 day' from {{ this }})
    {% else %}
        {{ filter_recent_data('creation_date') }}
    {% endif %}
)

select
    casting_log.id
    , casting_log.tenant_id
    , casting_log.guarantor_id
    , casting_log.operator_id
    , casting_log.log_type
    , casting_log.created_at
    , casting_log.edition_type
    , casting_log.document_id
    , casting_log.document_category
    , casting_log.document_sub_category
    , casting_log.operator_comment
    , case when casting_log.guarantor_id is not null then 'GUARANTOR' else 'TENANT' end as tenant_type
from casting_log
