{{ config(
      materialized='incremental'
    , incremental_strategy='delete+insert'
    , unique_key = 'order_date'
    , pre_hook = '{{ incremental_warehouse_size() }}'
)}}


with 
{% if is_incremental() %}
    incremental_orders as (
    select
        distinct id
    from raw.jaffle_shop.orders
        where order_date >= (select max(order_date) from {{this}})
    ),

    compute_incremental_orders as (
    select
        id,
        user_id,
        order_date,
        status
    from raw.jaffle_shop.orders src
    where exists (
        select 1
        from incremental_orders inc
        where
        inc.id = src.id
     )
    )

{% else %}
    orders as (
    select
        id,
        user_id,
        order_date,
        status
    from raw.jaffle_shop.orders
        where order_date >= '2018-01-02'::date
    )
{% endif %}

select
    orders.order_date,
    count(distinct orders.id) ttl_orders,
    count(distinct customers.id) ttl_customers,
    sum(payment.amount) ttl_amount

{% if is_incremental() %}
from
    compute_incremental_orders as orders
    left join raw.jaffle_shop.customers customers on orders.user_id = customers.id
    left join raw.stripe.payment payment on orders.id = payment.orderid
    and payment.status = 'success'
where
    orders.status = 'completed'
    and order_date >= (select max(order_date) from {{this}})
{% else %}
    from orders
    left join raw.jaffle_shop.customers customers on orders.user_id = customers.id
    left join raw.stripe.payment payment on orders.id = payment.orderid
    and payment.status = 'success'
where 
    orders.status = 'completed'
    and order_date >= '2018-01-02'::date
{% endif %}

group by
    orders.order_date

