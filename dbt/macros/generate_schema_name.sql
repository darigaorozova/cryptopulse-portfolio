{# Схема модели = ровно то, что указано в +schema (без префикса target-схемы),
   чтобы объекты лежали в orozova_ods / orozova_datamart по неймингу ТЗ. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
