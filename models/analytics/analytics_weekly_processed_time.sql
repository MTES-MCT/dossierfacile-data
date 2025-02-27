with process_delay as (
    select
        id
        , DATE_TRUNC('week', first_completion_at) as completed_at
        , case
            when 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 <= 1 then '≤ 1 heure'
            when 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 > 1 and 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 <= 4 then '≤ 4 heures'
            when 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 > 4 and 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 <= 24 then '≤ 24 heures'
            when 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 > 24 and 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 <= 36 then '≤ 36 heures'
            when 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 > 36 and 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 <= 48 then '≤ 48 heures'
            when 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 > 48 and 1.0 * EXTRACT(epoch from first_operation_at - first_completion_at) / 3600 <= 168 then '≤ 1 semaine'
            else '> 1 semaine'
        end as time_to_process
    from {{ ref('core_tenant_account') }}
    where
        nb_completions > 0 -- avec flag de completion par l'utilisateur
        and first_completion_at >= DATE_TRUNC('WEEK', CURRENT_DATE) - INTERVAL '53 WEEK'
        and first_completion_at < DATE_TRUNC('WEEK', CURRENT_DATE)
        and first_operation_at is not null
)

select
    completed_at
    , time_to_process
    , COUNT(distinct id)
from process_delay
group by completed_at, time_to_process
order by completed_at, time_to_process
