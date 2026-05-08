with orders as (
    select * from {{ ref('stg_orders') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
orders_with_date as (
    select
        order_id,
        store_id,
        cast(ordered_at as date) as ordered_date
    from orders
),
store_daily_sales as (
    select
        o.store_id,
        o.ordered_date,
        p.product_type,
        count(distinct o.order_id) as order_count,
        count(oi.order_item_id) as item_count,
        coalesce(sum(case when oi.quantity is not null and p.price is not null then oi.quantity * p.price end), 0) as gross_sales
    from orders_with_date o
    left join order_items oi on o.order_id = oi.order_id
    left join products p on oi.sku = p.sku
    group by o.store_id, o.ordered_date, p.product_type
)

select * from store_daily_sales
