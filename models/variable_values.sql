{% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
{% set default_warehouse = target.warehouse %}

select 
    '{{relation}}' as the_relation,
    '{{this.database}}' as the_database,
    '{{this.schema}}' as the_schema,
    '{{this.table}}' as the_table,
    '{{default_warehouse}}' as the_warehouse
