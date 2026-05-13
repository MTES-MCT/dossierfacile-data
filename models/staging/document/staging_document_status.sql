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
        -- Bugfix: document_id is null for some tenant_log entries created before 2025
        and document_id is not null
)

, document_status as (
    select distinct
        document_id
        -- bugfix: When the primary tenant deletes a document belonging to a couple,
        -- the tenant_log is created using the primary tenant_id rather than the associated tenant_id.
        -- fixed in production data after 2026-05-05 (see https://github.com/MTES-MCT/dossierfacile-backend/pull/1232)
        -- note: this fix does not resolve cases where a couple's document history begins with a DELETE_DOCUMENT event in tenant_log...
        , FIRST_VALUE(tenant_id) over (
            partition by document_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as tenant_id
        , guarantor_id
        , document_category
        , document_sub_category
        , FIRST_VALUE(created_at) over (
            partition by document_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as first_created_at
        , LAST_VALUE(created_at) over (
            partition by document_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as last_created_at
        , LAST_VALUE(edition_type) over (
            partition by document_id
            order by created_at asc
            rows between unbounded preceding and unbounded following
        ) as last_document_events
        , SUM(case when edition_type = 'ADD_DOCUMENT' then 1 else 0 end) over (
            partition by document_id
            rows between unbounded preceding and unbounded following
        ) as total_add_document_events
        , SUM(case when edition_type = 'DELETE_DOCUMENT' then 1 else 0 end) over (
            partition by document_id
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
