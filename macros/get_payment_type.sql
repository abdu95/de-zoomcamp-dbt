{% macro get_payment_type(payment_type_column) %}
    CASE {{ payment_type_column }}
    {% set payment_types = dbt_utils.get_column_values(table=ref('payment_type_lookup'), column='payment_type') %}
    {% set descriptions = dbt_utils.get_column_values(table=ref('payment_type_lookup'), column='description') %}
    {% for i in range(payment_types | length) %}
        WHEN {{ payment_types[i] }} THEN '{{ descriptions[i] }}'
    {% endfor %}
        ELSE 'Unknown'
    END
{% endmacro %}