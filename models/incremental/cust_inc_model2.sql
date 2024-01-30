{{ config(
      materialized='incremental'
    , incremental_strategy='delete+insert'
    , unique_key = 'order_date'
    , pre_hook = '{{ incremental_warehouse_size() }}'
)}}


with orders as (
select
    id,
    user_id,
    order_date,
    status
from raw.jaffle_shop.orders
    {% if is_incremental() %}
       where order_date >= (select max(order_date) from {{this}})
    {% else %}
       where order_date >= '2018-01-02'::date
    {% endif %}
),
orders_summary as (
select
    orders.order_date,
    count(distinct orders.id) ttl_orders,
    count(distinct customers.id) ttl_customers,
    sum(payment.amount) ttl_amount
from
    orders as orders
    left join raw.jaffle_shop.customers customers on orders.user_id = customers.id
    left join raw.stripe.payment payment on orders.id = payment.orderid
    and payment.status = 'success'
where
    orders.status = 'completed'
group by
    orders.order_date
)
select *
from orders_summary
    {% if is_incremental() %}
       where order_date >= (select max(order_date) from {{this}})
    {% else %}
       where order_date >= '2018-01-02'::date
    {% endif %}
