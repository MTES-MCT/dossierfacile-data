with tenant_document_events as (
    select
        id
        , {{ dbt_utils.generate_surrogate_key(['tenant_type', 'tenant_id', 'guarantor_id']) }} as tenant_unique_id
        , tenant_id
        , guarantor_id
        , tenant_type
        , created_at
        , document_category
        , document_sub_category
    from {{ ref('staging_tenant_log') }}
    -- log_type = 'ACCOUNT_EDITED' and edition_type = 'ADD_DOCUMENT'
    where edition_type = 'ADD_DOCUMENT'
)

, last_tenant_document_status as (
    select distinct
        tenant_unique_id
        , tenant_id
        , guarantor_id
        , tenant_type
        , document_category
        , FIRST_VALUE(created_at) over (
            partition by tenant_unique_id, document_category
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as first_added_at
        , LAST_VALUE(document_sub_category) over (
            partition by tenant_unique_id, document_category
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as last_document_sub_category
    from tenant_document_events
)

, pivot_tenant_document_status as (
    select
        tenant_unique_id
        , tenant_id
        , guarantor_id
        , tenant_type

        , case when document_category = 'IDENTIFICATION' then last_document_sub_category end as identification_last_sub_category
        , case when document_category = 'IDENTIFICATION' then first_added_at end as identification_first_added_at

        , case when document_category = 'FINANCIAL' then last_document_sub_category end as financial_last_sub_category
        , case when document_category = 'FINANCIAL' then first_added_at end as financial_first_added_at

        , case when document_category = 'RESIDENCY' then last_document_sub_category end as residency_last_sub_category
        , case when document_category = 'RESIDENCY' then first_added_at end as residency_first_added_at

        , case when document_category = 'PROFESSIONAL' then last_document_sub_category end as professional_last_sub_category
        , case when document_category = 'PROFESSIONAL' then first_added_at end as professional_first_added_at

        , case when document_category = 'TAX' then last_document_sub_category end as tax_last_sub_category
        , case when document_category = 'TAX' then first_added_at end as tax_first_added_at
    from last_tenant_document_status
)

, tenant_document_table as (
    select
        tenant_unique_id
        , tenant_id
        , guarantor_id
        , tenant_type

        , MAX(identification_first_added_at) as identification_first_added_at
        , MAX(identification_last_sub_category) as identification_last_sub_category

        , MAX(financial_first_added_at) as financial_first_added_at
        , MAX(financial_last_sub_category) as financial_last_sub_category

        , MAX(residency_first_added_at) as residency_first_added_at
        , MAX(residency_last_sub_category) as residency_last_sub_category

        , MAX(professional_first_added_at) as professional_first_added_at
        , MAX(professional_last_sub_category) as professional_last_sub_category

        , MAX(tax_first_added_at) as tax_first_added_at
        , MAX(tax_last_sub_category) as tax_last_sub_category
    from pivot_tenant_document_status
    group by
        tenant_unique_id
        , tenant_id
        , guarantor_id
        , tenant_type
)

select
    *
    , case when identification_first_added_at is null then 0 else 1 end as has_identification_document
    , case when financial_first_added_at is null then 0 else 1 end as has_financial_document
    , case when residency_first_added_at is null then 0 else 1 end as has_residency_document
    , case when professional_first_added_at is null then 0 else 1 end as has_professional_document
    , case when tax_first_added_at is null then 0 else 1 end as has_tax_document
    , case when (
        identification_first_added_at is not null
        and financial_first_added_at is not null
        and residency_first_added_at is not null
        and professional_first_added_at is not null
        and tax_first_added_at is not null
    ) then 1 else 0 end as document_completion_flag
from tenant_document_table
