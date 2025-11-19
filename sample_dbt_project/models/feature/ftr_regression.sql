-- Big flat table for regression ML model
with dim_customers as (
    select * from {{ ref('dim_customers') }}
),
fct_orders as (
    select * from {{ ref('fct_orders') }}
),

customer_lifetime_value as (
    select
        c.customer_id,
        c.customer_name,
        c.order_count,
        c.first_ordered_at,
        c.last_ordered_at,
        sum(o.order_total) as total_spent,
        avg(o.order_total) as avg_order_value,
        -- Regression target: total spent by customer
        sum(o.order_total) as target_lifetime_value
    from dim_customers c
    left join fct_orders o on c.customer_id = o.customer_id
    group by c.customer_id, c.customer_name, c.order_count, c.first_ordered_at, c.last_ordered_at
)

select * from customer_lifetime_value;
