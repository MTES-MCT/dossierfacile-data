with document_events as (
    select
        id
        , document_id
        , COALESCE(document_tenant_id, tenant_id) as tenant_id
        , guarantor_id
        , created_at
        , document_category
        , document_sub_category
        , edition_type
    from {{ ref('staging_tenant_log') }}
    where
        log_type in ('ACCOUNT_EDITED', 'DOCUMENT_ADDED', 'DOCUMENT_DELETED')
        and edition_type in ('ADD_DOCUMENT', 'DELETE_DOCUMENT')
        -- Bugfix: document_id is null for some tenant_log entries created before 2025
        and document_id is not null
)

, document_status as (
    select
        id
        , document_id
        -- Keep the first retained owner for legacy logs whose deletion actor may differ.
        -- Without an explicit owner or creation log, ownership can remain incomplete.
        , FIRST_VALUE(tenant_id) over document_history as tenant_id
        , FIRST_VALUE(guarantor_id) over document_history as guarantor_id
        , document_category
        , document_sub_category
        , created_at
        , edition_type
        , FIRST_VALUE(created_at) over document_history as first_created_at
        , LAST_VALUE(id) over document_history as last_event_id
        , COUNT(*) filter (where edition_type = 'ADD_DOCUMENT') over document_history as total_add_document_events
        , COUNT(*) filter (where edition_type = 'DELETE_DOCUMENT') over document_history as total_delete_document_events
    from document_events
    window document_history as (
        partition by document_id
        order by created_at asc, id asc
        rows between unbounded preceding and unbounded following
    )
)

select
    document_id
    , tenant_id
    , guarantor_id
    , document_category
    , document_sub_category
    , case
        when edition_type = 'ADD_DOCUMENT' then 'ADDED'
        when edition_type = 'DELETE_DOCUMENT' then 'DELETED'
    end as document_status
    , first_created_at as added_at
    , case
        when edition_type = 'DELETE_DOCUMENT' then created_at
    end as deleted_at
    , total_add_document_events
    , total_delete_document_events
from document_status
-- Use the latest event's metadata so changing attributes cannot duplicate a document.
where id = last_event_id
