{% test min_timestamp_gte_fixed(model, column_name, value) %}
    select *
    from {{ model }}
    where {{ column_name }} < try_to_timestamp_ntz({{ "'" ~ value ~ "'" }})
{% endtest %}