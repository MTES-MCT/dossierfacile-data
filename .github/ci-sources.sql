-- Empty source tables for the disposable PostgreSQL service in CI.

create table public.apartment_sharing (
    id BIGINT
    , operator_date TIMESTAMP WITHOUT TIME ZONE
    , application_type CHARACTER VARYING
    , dossier_pdf_document_status CHARACTER VARYING
    , last_update_date TIMESTAMP WITHOUT TIME ZONE
    , pdf_dossier_file_id BIGINT
);

create table public.apartment_sharing_link (
    id BIGINT
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , apartment_sharing_id BIGINT
    , full_data BOOLEAN
    , disabled BOOLEAN
    , link_type CHARACTER VARYING
    , last_sent_datetime TIMESTAMP WITHOUT TIME ZONE
    , expiration_date TIMESTAMP WITHOUT TIME ZONE
    , deleted BOOLEAN
    , partner_id BIGINT
    , property_id BIGINT
    , token TEXT
);

create table public.document (
    id BIGINT
    , document_category CHARACTER VARYING
    , tenant_id BIGINT
    , monthly_sum INTEGER
    , guarantor_id BIGINT
    , document_sub_category CHARACTER VARYING
    , document_status CHARACTER VARYING
    , no_document BOOLEAN
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , watermark_file_id BIGINT
    , last_modified_date TIMESTAMP WITHOUT TIME ZONE
    , document_category_step CHARACTER VARYING
);

create table public.document_analysis_report (
    id BIGINT
    , document_id BIGINT
    , analysis_status CHARACTER VARYING
    , failed_rules JSONB
    , passed_rules JSONB
    , inconclusive_rules JSONB
    , created_at TIMESTAMP WITHOUT TIME ZONE
    , data_document_id BIGINT
);

create table public.document_denied_options (
    id INTEGER
    , message_value TEXT
    , document_sub_category CHARACTER VARYING
    , document_user_type CHARACTER VARYING
    , code CHARACTER VARYING
    , document_category CHARACTER VARYING
);

create table public.document_denied_reasons (
    id BIGINT
    , checked_options CHARACTER VARYING[]
    , checked_options_id INTEGER[]
    , message_data BOOLEAN
    , document_id BIGINT
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , document_category CHARACTER VARYING
    , document_sub_category CHARACTER VARYING
    , document_category_step CHARACTER VARYING
    , document_tenant_type CHARACTER VARYING
);

create table public.feature_flag (
    key CHARACTER VARYING
    , description TEXT
    , active BOOLEAN
    , only_for_new_user BOOLEAN
    , rollout_pct INTEGER
    , deployment_date TIMESTAMP WITHOUT TIME ZONE
    , created_at TIMESTAMP WITHOUT TIME ZONE
    , updated_at TIMESTAMP WITHOUT TIME ZONE
);

create table public.file (
    id BIGINT
    , document_id BIGINT
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , number_of_pages INTEGER
    , preview_file_id BIGINT
    , storage_file_id BIGINT
);

create table public.guarantor (
    id BIGINT
    , tenant_id BIGINT
    , type_guarantor CHARACTER VARYING
);

create table public.link_log (
    id BIGINT
    , apartment_sharing_id BIGINT
    , link_type CHARACTER VARYING
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , token TEXT
);

create table public.operator_log (
    id BIGINT
    , tenant_id BIGINT
    , operator_id BIGINT
    , tenant_status CHARACTER VARYING
    , action_type CHARACTER VARYING
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , processed_documents INTEGER
    , time_spent INTEGER
);

create table public.property (
    id BIGINT
    , owner_id BIGINT
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , count_visit INTEGER
    , property_id CHARACTER VARYING
    , displayed BOOLEAN
    , validated BOOLEAN
    , type CHARACTER VARYING
    , validated_date TIMESTAMP WITHOUT TIME ZONE
    , dpe_not_required BOOLEAN
);

create table public.property_log (
    id BIGINT
    , property_id BIGINT
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , log_type CHARACTER VARYING
    , apartment_sharing_id BIGINT
);

create table public.tenant (
    id BIGINT
    , tenant_type CHARACTER VARYING
    , apartment_sharing_id BIGINT
    , zip_code CHARACTER VARYING
    , honor_declaration BOOLEAN
    , last_update_date TIMESTAMP WITHOUT TIME ZONE
    , status CHARACTER VARYING
    , warnings INTEGER
    , owner_type CHARACTER VARYING
    , validation_requested BOOLEAN
    , ready_for_auto_validation BOOLEAN
);

create table public.tenant_log (
    id BIGINT
    , tenant_id BIGINT
    , operator_id BIGINT
    , log_type CHARACTER VARYING
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , log_details JSONB
);

create table public.tenant_userapi (
    tenant_id BIGINT
    , userapi_id BIGINT
    , access_granted_date TIMESTAMP WITHOUT TIME ZONE
);

create table public.user_account (
    id BIGINT
    , creation_date TIMESTAMP WITHOUT TIME ZONE
    , last_login_date TIMESTAMP WITHOUT TIME ZONE
    , update_date_time TIMESTAMP WITHOUT TIME ZONE
    , enabled BOOLEAN
    , france_connect BOOLEAN
    , user_type CHARACTER VARYING
    , acquisition_campaign CHARACTER VARYING
    , acquisition_source CHARACTER VARYING
    , acquisition_medium CHARACTER VARYING
);

create table public.user_api (
    id BIGINT
    , name CHARACTER VARYING
    , name2 CHARACTER VARYING
    , disabled BOOLEAN
);

create table public.user_feature_assignment_history (
    id BIGINT
    , user_id BIGINT
    , feature_key CHARACTER VARYING
    , enabled BOOLEAN
    , bucket INTEGER
    , rollout_pct INTEGER
    , changed_at TIMESTAMP WITHOUT TIME ZONE
    , reason CHARACTER VARYING
);

create table public.user_operator (
    id BIGINT
    , created_at TIMESTAMP WITHOUT TIME ZONE
    , email CHARACTER VARYING
    , first_name CHARACTER VARYING
    , last_name CHARACTER VARYING
    , last_login_date TIMESTAMP WITHOUT TIME ZONE
    , updated_at TIMESTAMP WITHOUT TIME ZONE
);

create table public.watermark_document (
    id BIGINT
    , created_date TIMESTAMP WITHOUT TIME ZONE
    , pdf_status CHARACTER VARYING
    , pdf_file_id BIGINT
);
