{% macro incremental_warehouse_size() %}
{% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
{% set default_warehouse = target.warehouse %}
        {% if (relation is none or flags.FULL_REFRESH) %}
            {{ return ("USE WAREHOUSE TRANSFORMING")}}
        {% else %}
            {{ return("USE WAREHOUSE " ~default_warehouse) }}
        {% endif %}
{% endmacro %}