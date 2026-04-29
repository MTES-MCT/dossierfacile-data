with document_events as (
    select
        id
        , document_id
        , tenant_id
        , guarantor_id
        , created_at
        , document_category
        , document_sub_category
        , edition_type
    from {{ ref('staging_tenant_log') }}
    where
        log_type = 'ACCOUNT_EDITED'
        and edition_type in ('ADD_DOCUMENT', 'DELETE_DOCUMENT')
)

, document_status as (
    select distinct
        document_id
        , tenant_id
        , guarantor_id
        , document_category
        , document_sub_category
        , FIRST_VALUE(created_at) over (
            partition by document_id, tenant_id, guarantor_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as first_created_at
        , LAST_VALUE(created_at) over (
            partition by document_id, tenant_id, guarantor_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as last_created_at
        , LAST_VALUE(edition_type) over (
            partition by document_id, tenant_id, guarantor_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as last_document_events
        , SUM(case when edition_type = 'ADD_DOCUMENT' then 1 else 0 end) over (
            partition by document_id, tenant_id, guarantor_id
            rows between unbounded preceding and unbounded following
        ) as total_add_document_events
        , SUM(case when edition_type = 'DELETE_DOCUMENT' then 1 else 0 end) over (
            partition by document_id, tenant_id, guarantor_id
            rows between unbounded preceding and unbounded following
        ) as total_delete_document_events
    from document_events
)

select
    document_id
    , tenant_id
    , guarantor_id
    , document_category
    , document_sub_category
    , case
        when last_document_events = 'ADD_DOCUMENT' then 'ADDED'
        when last_document_events = 'DELETE_DOCUMENT' then 'DELETED'
    end as document_status
    , first_created_at as added_at
    , case
        when last_document_events = 'DELETE_DOCUMENT' then last_created_at
    end as deleted_at
    , total_add_document_events
    , total_delete_document_events
from document_status
