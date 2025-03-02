-- tests/no_negative_timetable_variation.sql
{% test is_positive(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{column_name}} < 0
{% endtest %}
