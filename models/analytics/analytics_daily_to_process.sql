-- unique_key est utile si la stratégie est 'merge'
{{ config(
    materialized='incremental',
    unique_key='date_jour' 
) }}

select
    CURRENT_DATE as date_jour
    , COUNT(*) as valeur_incremente
from {{ ref('core_tenant_account') }}
where
    status = 'TO_PROCESS'
    {% if is_incremental() %}
        and CURRENT_DATE > (select COALESCE(MAX(date_jour), '1900-01-01') from  {{this}})
    {% endif %}
