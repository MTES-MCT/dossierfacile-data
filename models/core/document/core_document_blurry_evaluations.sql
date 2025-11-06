with blurry_rule_reports as (
    select
        unique_id
        , document_id
        , created_at
        , rule_name
        , rule_status
        , rule_level
        , overall_status
        , tenant_comment
    from {{ ref('staging_document_analysis_report') }}
    where
        rule_name = 'R_BLURRY_FILE'
        and document_id > {{ var("blurry_detection_first_document_id") }}
)

, blurry_denied_reasons as (
    select
        document_id
        , denied_option_id
        , document_category
        , document_sub_category
        , document_category_step
        , document_tenant_type
        , document_denied_at
        , operator_comment
    from {{ ref('staging_document_denied_reasons') }}
    where
        document_id > {{ var("blurry_detection_first_document_id") }}
        and denied_option_id = 567
)

, blurry_document_analysis as (
    select
        document_id
        , BOOL_OR(is_blurry) as is_blurry -- if any file is blurry, the document is blurry
        , BOOL_OR(is_blank) as is_blank -- if any file is blank, the document is blank
        , MIN(ocr_token_count) as min_ocr_token_count
        , MIN(ocr_mean_score) as min_ocr_mean_score
    from {{ ref('staging_file_blurry_analysis') }}
    where
        document_id > {{ var("blurry_detection_first_document_id") }}
    group by document_id
)

, document as (
    select
        id
        , document_category
        , document_sub_category
        , document_category_step
        , document_status
        , case when tenant_id is not null then 'TENANT' else 'GUARANTOR' end as tenant_type
    from {{ ref('staging_document') }}
    where
        id > {{ var("blurry_detection_first_document_id") }}
        and document_status in ('VALIDATED', 'DECLINED')
)

, blurry_detection_evaluation_dataset as (
    select
        document.document_status
        , blurry_denied_reasons.document_denied_at
        , blurry_rule_reports.created_at as blurry_rule_created_at
        , blurry_document_analysis.is_blurry
        , blurry_document_analysis.is_blank
        , blurry_document_analysis.min_ocr_token_count
        , blurry_document_analysis.min_ocr_mean_score

        , COALESCE(blurry_denied_reasons.document_id, document.id) as document_id
        , COALESCE(blurry_denied_reasons.document_category, document.document_category) as document_category
        , COALESCE(blurry_denied_reasons.document_sub_category, document.document_sub_category) as document_sub_category
        , COALESCE(blurry_denied_reasons.document_category_step, document.document_category_step) as document_category_step
        , UPPER(COALESCE(blurry_denied_reasons.document_tenant_type, document.tenant_type)) as document_tenant_type
        , case
            when blurry_rule_reports.rule_status = 'FAILED' then 1
            when blurry_rule_reports.rule_status = 'PASSED' then 0
            when blurry_rule_reports.rule_status = 'INCONCLUSIVE' then null
        end as blur_prediction

        , case
            -- si le document a un blurry denied reason, alors il a été rejeté comme blurry par les opérateurs
            when blurry_denied_reasons.document_id is not null then 1
            -- si le document n'a pas de denied reason, alors :
            -- - il a été vu et validé par un opérateur (non blurry, status = 'VALIDATED' or 'DECLINED') (présent dans la table document)

            -- - le cas: il n'a pas encore été vu par un opérateur (status = 'IN_PROGRESS' | 'INCOMPLETE'), n'existe pas puisqu'on filtre sur les documents VALIDATED ou DECLINED
            -- - le cas: il a été supprimé par l'utilisateur avant son passage en BO (status = 'DELETED'), est encore possible, c'est une limitation du modèle
            -- hypothèse: moyennant cette limitation, on peut considérer comme une vraie ground truth
            else 0
        end as blur_ground_truth

    from blurry_rule_reports
    left join blurry_denied_reasons
        on blurry_rule_reports.document_id = blurry_denied_reasons.document_id
    left join blurry_document_analysis
        on blurry_rule_reports.document_id = blurry_document_analysis.document_id
    left join document
        on blurry_rule_reports.document_id = document.id
    where COALESCE(blurry_denied_reasons.document_id, document.id) is not null
)

select
    *
    -- Model predicted blurry AND it was actually blurry (correct positive prediction)
    , case when blur_prediction = 1 and blur_ground_truth = 1 then 1 else 0 end as true_positive
    -- Model predicted blurry BUT it was actually not blurry (incorrect positive prediction)
    , case when blur_prediction = 1 and blur_ground_truth = 0 then 1 else 0 end as false_positive
    -- Model predicted not blurry AND it was actually not blurry (correct negative prediction)
    , case when blur_prediction = 0 and blur_ground_truth = 0 then 1 else 0 end as true_negative
    -- Model predicted not blurry BUT it was actually blurry (incorrect negative prediction)
    , case when blur_prediction = 0 and blur_ground_truth = 1 then 1 else 0 end as false_negative
from blurry_detection_evaluation_dataset
