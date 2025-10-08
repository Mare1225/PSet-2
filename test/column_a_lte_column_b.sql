{% test column_a_lte_column_b(model, column_name, column_b) %}
    select *
    from {{ model }}
    where {{ column_name }} < {{ column_b }}
{% endtest %}