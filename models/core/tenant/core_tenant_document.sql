with tenant_documents as (
    select *
    from {{ ref('core_document') }}
    where guarantor_id is null
)

, pivot_tenant_documents as (
    select
        document_id
        , tenant_id
        , created_at
        , deleted_at
        , document_status
        , monthly_net_income
        , case when document_category = 'IDENTIFICATION' then document_sub_category end as identification_sub_category
        , case when document_category = 'FINANCIAL' then document_sub_category end as financial_sub_category
        , case when document_category = 'RESIDENCY' then document_sub_category end as residency_sub_category
        , case when document_category = 'PROFESSIONAL' then document_sub_category end as professional_sub_category
        , case when document_category = 'TAX' then document_sub_category end as tax_sub_category
        , case when document_category = 'IDENTIFICATION' then document_status end as identification_document_status
        , case when document_category = 'FINANCIAL' then document_status end as financial_document_status
        , case when document_category = 'RESIDENCY' then document_status end as residency_document_status
        , case when document_category = 'PROFESSIONAL' then document_status end as professional_document_status
        , case when document_category = 'TAX' then document_status end as tax_document_status
    from tenant_documents
    where document_status <> 'DELETED'
)

, tenant_document_status as (
    select distinct
        tenant_id
        , COUNT(identification_sub_category) over (partition by tenant_id) as nb_identification_documents
        , LAST_VALUE(identification_sub_category) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as identification_sub_category
        , LAST_VALUE(identification_document_status) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as identification_document_status

        , COUNT(financial_sub_category) over (partition by tenant_id) as nb_financial_documents
        , LAST_VALUE(financial_sub_category) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as financial_sub_category
        , LAST_VALUE(financial_document_status) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as financial_document_status

        , COUNT(residency_sub_category) over (partition by tenant_id) as nb_residency_documents
        , LAST_VALUE(residency_sub_category) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as residency_sub_category
        , LAST_VALUE(residency_document_status) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as residency_document_status

        , COUNT(professional_sub_category) over (partition by tenant_id) as nb_professional_documents
        , LAST_VALUE(professional_sub_category) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as professional_sub_category
        , LAST_VALUE(professional_document_status) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as professional_document_status

        , COUNT(tax_sub_category) over (partition by tenant_id) as nb_tax_documents
        , LAST_VALUE(tax_sub_category) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as tax_sub_category
        , LAST_VALUE(tax_document_status) over (
            partition by tenant_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as tax_document_status

        , SUM(monthly_net_income) over (partition by tenant_id) as monthly_net_income
    from pivot_tenant_documents
)

select
    *
    , case when nb_identification_documents = 0 then 0 else 1 end as has_identification_document
    , case when nb_financial_documents = 0 then 0 else 1 end as has_financial_document
    , case when nb_residency_documents = 0 then 0 else 1 end as has_residency_document
    , case when nb_professional_documents = 0 then 0 else 1 end as has_professional_document
    , case when nb_tax_documents = 0 then 0 else 1 end as has_tax_document
    , case when (
        nb_identification_documents > 0
        and nb_financial_documents > 0
        and nb_residency_documents > 0
        and nb_professional_documents > 0
        and nb_tax_documents > 0
    ) then 1 else 0 end as document_completion_flag
from tenant_document_status
