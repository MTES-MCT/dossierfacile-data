with guarantor_documents as (
    select *
    from {{ ref('core_document') }}
    where guarantor_id is not null
)

, active_guarantor_documents as (
    select *
    from guarantor_documents
    where document_status <> 'DELETED'
)

, guarantor_document_status as (
    select
        guarantor_id
        , tenant_id
        , COUNT(*) filter (
            where document_category = 'IDENTIFICATION'
        ) as nb_identification_documents
        , (
            ARRAY_AGG(
                document_sub_category
                order by created_at desc, document_id desc
            ) filter (where document_category = 'IDENTIFICATION')
        )[1] as identification_sub_category
        , (
            ARRAY_AGG(
                document_status
                order by created_at desc, document_id desc
            ) filter (where document_category = 'IDENTIFICATION')
        )[1] as identification_document_status

        , COUNT(*) filter (
            where document_category = 'FINANCIAL'
        ) as nb_financial_documents
        , (
            ARRAY_AGG(
                document_sub_category
                order by created_at desc, document_id desc
            ) filter (where document_category = 'FINANCIAL')
        )[1] as financial_sub_category
        , (
            ARRAY_AGG(
                document_status
                order by created_at desc, document_id desc
            ) filter (where document_category = 'FINANCIAL')
        )[1] as financial_document_status

        , COUNT(*) filter (
            where document_category = 'RESIDENCY'
        ) as nb_residency_documents
        , (
            ARRAY_AGG(
                document_sub_category
                order by created_at desc, document_id desc
            ) filter (where document_category = 'RESIDENCY')
        )[1] as residency_sub_category
        , (
            ARRAY_AGG(
                document_status
                order by created_at desc, document_id desc
            ) filter (where document_category = 'RESIDENCY')
        )[1] as residency_document_status

        , COUNT(*) filter (
            where document_category = 'PROFESSIONAL'
        ) as nb_professional_documents
        , (
            ARRAY_AGG(
                document_sub_category
                order by created_at desc, document_id desc
            ) filter (where document_category = 'PROFESSIONAL')
        )[1] as professional_sub_category
        , (
            ARRAY_AGG(
                document_status
                order by created_at desc, document_id desc
            ) filter (where document_category = 'PROFESSIONAL')
        )[1] as professional_document_status

        , COUNT(*) filter (
            where document_category = 'TAX'
        ) as nb_tax_documents
        , (
            ARRAY_AGG(
                document_sub_category
                order by created_at desc, document_id desc
            ) filter (where document_category = 'TAX')
        )[1] as tax_sub_category
        , (
            ARRAY_AGG(
                document_status
                order by created_at desc, document_id desc
            ) filter (where document_category = 'TAX')
        )[1] as tax_document_status

        , SUM(monthly_net_income) as monthly_net_income
    from active_guarantor_documents
    group by guarantor_id, tenant_id
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
from guarantor_document_status
