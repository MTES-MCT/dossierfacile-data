-- unique_key est utile si la stratégie est 'merge'
{{ config(
    materialized='incremental',
    unique_key='date_jour' 
) }}

select
    CURRENT_DATE AS date_jour,
    count(*) AS valeur_incremente
from {{ ref('core_tenant_account') }}
where status = 'TO_PROCESS'
{% if is_incremental() %}
and date_jour > (SELECT COALESCE(MAX(date_jour), '1900-01-01') FROM {{ this }})
{% endif %}

