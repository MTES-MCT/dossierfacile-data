{% macro filter_recent_data(date_field) %}
    {% if target.name != 'prod' %}
        WHERE {{ date_field }} >= current_date - interval '6 week' 
    {% endif %}
{% endmacro %}
