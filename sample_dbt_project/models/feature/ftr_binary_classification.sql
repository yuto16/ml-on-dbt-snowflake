-- Big flat table for binary classification ML model
with dim_customers as (
    select * from {{ ref('dim_customers') }}
),
fct_orders as (
    select * from {{ ref('fct_orders') }}
)
,
customer_features as (
    select
        c.customer_id,
        c.customer_name,
        c.order_count,
        c.first_ordered_at,
        c.last_ordered_at,
        count(case when o.order_id is not null and o.subtotal > 0 then 1 end) as total_orders,
        sum(o.order_total) as total_spent,
        count(case when o.order_id is not null and o.subtotal > 0 and o.order_total > 0 then 1 end) as paid_orders,
        count(case when o.order_id is not null and o.subtotal > 0 and o.order_total = 0 then 1 end) as free_orders,
        -- Binary target: whether customer has ever placed a free order
        case when count(case when o.order_id is not null and o.order_total = 0 then 1 end) > 0 then 1 else 0 end as has_free_order
    from dim_customers c
    left join fct_orders o on c.customer_id = o.customer_id
    group by c.customer_id, c.customer_name, c.order_count, c.first_ordered_at, c.last_ordered_at
)

select * from customer_features
