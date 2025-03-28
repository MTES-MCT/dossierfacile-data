{% macro filter_recent_data(date_field) %}
    {% if target.name != 'prod' %}
        WHERE {{ date_field }} >= '2024-12-23 00:00:00'
        and {{ date_field }} < '2024-12-24 00:00:00'
    {% else %}
        WHERE {{ date_field }} >= '2023-01-01 00:00:00'
    {% endif %}
{% endmacro %}
