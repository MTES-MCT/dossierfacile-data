{% macro filter_recent_data(date_field) %}
    {% if target.name != 'prod' %}
        WHERE {{ date_field }} >= current_date - interval '8 week'
    {% else %}
        WHERE {{ date_field }} >= '2023-01-01 00:00:00'
    {% endif %}
{% endmacro %}
