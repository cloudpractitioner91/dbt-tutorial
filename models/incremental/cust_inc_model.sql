{{ config(
      materialized='incremental'
    , incremental_strategy='delete+insert'
    , unique_key = 'order_date'
)}}


with orders_summary as (
select
    orders.order_date,
    count(distinct orders.id) ttl_orders,
    count(distinct customers.id) ttl_customers,
    sum(payment.amount) ttl_amount
from
    raw.jaffle_shop.orders orders
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
