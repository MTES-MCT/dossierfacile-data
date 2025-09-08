select
    DATE(blurry_rule_created_at)
    , SUM(true_positive) as true_positive
    , SUM(false_positive) as false_positive
    , SUM(true_negative) as true_negative
    , SUM(false_negative) as false_negative
    -- Precision: Of all documents predicted as blurry, how many were actually blurry?
    , case
        when SUM(true_positive) + SUM(false_positive) > 0
            then ROUND(SUM(true_positive)::DECIMAL / (SUM(true_positive) + SUM(false_positive)), 4)
        else 0
    end as precision
    -- Recall: Of all actually blurry documents, how many did we catch?
    , case
        when SUM(true_positive) + SUM(false_negative) > 0
            then ROUND(SUM(true_positive)::DECIMAL / (SUM(true_positive) + SUM(false_negative)), 4)
        else 0
    end as recall
    , case
        when SUM(true_positive) + SUM(false_positive) + SUM(false_negative) > 0
            then ROUND((2 * SUM(true_positive)::DECIMAL) / (2 * SUM(true_positive) + SUM(false_positive) + SUM(false_negative)), 4)
        else 0
    end as f1_score
from {{ ref('core_document_blurry_evaluations') }}
group by
    DATE(blurry_rule_created_at)
order by
    DATE(blurry_rule_created_at) desc
