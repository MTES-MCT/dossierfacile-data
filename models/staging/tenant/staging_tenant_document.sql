with pivot_document_table as (
    select
        id
        , document_category
        , tenant_id
        , modified_at
        , case when document_status = 'VALIDATED' then monthly_net_income end as validated_net_income

        , case when document_category = 'IDENTIFICATION' then document_status end as identification_document_status
        , case when document_category = 'IDENTIFICATION' then document_sub_category end as identification_sub_category

        , case when document_category = 'FINANCIAL' then document_status end as financial_document_status
        , case when document_category = 'FINANCIAL' then document_sub_category end as financial_sub_category

        , case when document_category = 'RESIDENCY' then document_status end as residency_document_status
        , case when document_category = 'RESIDENCY' then document_sub_category end as residency_sub_category

        , case when document_category = 'PROFESSIONAL' then document_status end as professional_document_status
        , case when document_category = 'PROFESSIONAL' then document_sub_category end as professional_sub_category

        , case when document_category = 'TAX' then document_status end as tax_document_status
        , case when document_category = 'TAX' then document_sub_category end as tax_sub_category
    from {{ ref('staging_document') }}
)

, last_status_document_table as (
    select distinct
        tenant_id
        , document_category
        , SUM(validated_net_income) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as sum_validated_net_income

        , LAST_VALUE(identification_document_status) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_identification_document_status
        , LAST_VALUE(identification_sub_category) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_identification_sub_category

        , LAST_VALUE(financial_document_status) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_financial_document_status
        , LAST_VALUE(financial_sub_category) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_financial_sub_category

        , LAST_VALUE(residency_document_status) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_residency_document_status
        , LAST_VALUE(residency_sub_category) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_residency_sub_category

        , LAST_VALUE(professional_document_status) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_professional_document_status
        , LAST_VALUE(professional_sub_category) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_professional_sub_category

        , LAST_VALUE(tax_document_status) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_tax_document_status
        , LAST_VALUE(tax_sub_category) over (
            partition by tenant_id, document_category
            order by modified_at asc
            rows between unbounded preceding and unbounded following
        ) as last_tax_sub_category

    from pivot_document_table
)

, tenant_document_table as (
    select
        tenant_id
        , MAX(sum_validated_net_income) as validated_net_income

        , MAX(last_identification_document_status) as identification_document_status
        , MAX(last_identification_sub_category) as identification_sub_category

        , MAX(last_financial_document_status) as financial_document_status
        , MAX(last_financial_sub_category) as financial_sub_category

        , MAX(last_residency_document_status) as residency_document_status
        , MAX(last_residency_sub_category) as residency_sub_category

        , MAX(last_professional_document_status) as professional_document_status
        , MAX(last_professional_sub_category) as professional_sub_category

        , MAX(last_tax_document_status) as tax_document_status
        , MAX(last_tax_sub_category) as tax_sub_category
    from last_status_document_table
    group by tenant_id
)

select
    *
    , case when identification_document_status is null then 0 else 1 end as has_identification_document
    , case when financial_document_status is null then 0 else 1 end as has_financial_document
    , case when residency_document_status is null then 0 else 1 end as has_residency_document
    , case when professional_document_status is null then 0 else 1 end as has_professional_document
    , case when tax_document_status is null then 0 else 1 end as has_tax_document
from tenant_document_table
