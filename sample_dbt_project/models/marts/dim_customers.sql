
with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
)
select
    c.customer_id,
    c.customer_name,
    count(o.order_id) as order_count,
    min(o.ordered_at) as first_ordered_at,
    max(o.ordered_at) as last_ordered_at
from customers c
left join orders o on c.customer_id = o.customer_id
group by c.customer_id, c.customer_name
