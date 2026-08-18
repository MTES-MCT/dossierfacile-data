{{ config(
    materialized = 'incremental', 
    unique_key = 'id',
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['tenant_id']},
      {'columns': ['created_at'], 'type': 'brin'} 
    ]
) }}


select
    CAST(id as INTEGER)
    , CAST(tenant_id as INTEGER)
    , CAST(operator_id as INTEGER)
    , case
        when action_type = 'START_PROCESS' then 'ACCOUNT_VALIDATION_STARTED'
        when action_type = 'STOP_PROCESS' then 'ACCOUNT_VALIDATION_STOPPED'
        when action_type = 'VIEW_APPLICATION' then 'APPLICATION_VIEWED'
        when action_type = 'SEARCH_TENANT' then 'APPLICATION_SEARCHED'
    end as log_type
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(tenant_status as VARCHAR)
    , CAST(processed_documents as INTEGER)
    , CAST(time_spent as INTEGER)
from {{ source('dossierfacile', 'operator_log') }}
{{ filter_recent_data('creation_date') }}

{% if is_incremental() %}
    WHERE creation_date > (SELECT MAX(created_at) - INTERVAL '2 day' FROM {{ this }}) 
{% endif %}
