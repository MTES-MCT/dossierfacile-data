{% macro get_incremental_analyzed_delete_insert_sql(arg_dict) %}
    {# The freshly populated temporary table needs statistics for the delete's query plan. #}
    analyze {{ arg_dict["temp_relation"] }};
    {{ get_incremental_delete_insert_sql(arg_dict) }}
{% endmacro %}
