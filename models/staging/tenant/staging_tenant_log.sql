with casting_log as (
    select
        CAST(id as INTEGER)
        , CAST(tenant_id as INTEGER)
        , CAST(operator_id as INTEGER)
        , CAST(log_type as VARCHAR)
        , CAST(creation_date as TIMESTAMP) as created_at
        -- flag migrate dans tenant_log pour indiquer les lignes qui ont été migrées 
        -- utile si on veut faire qq chose de plus fin comme par exemple dupliquer le dernier logs de suppression en un log d’archivage et de suppression
        -- il y avait un bug précédemment qui ne permettait pas de distinguer la suppression quand un archivage avait eu lieu.
        , CAST(migrate as BOOLEAN)
    from {{ source('dossierfacile', 'tenant_log') }}
    {{ filter_recent_data('creation_date') }}
)

select
    casting_log.id
    , casting_log.tenant_id
    , casting_log.operator_id
    , casting_log.log_type
    , casting_log.created_at
    , casting_log.migrate
from casting_log
