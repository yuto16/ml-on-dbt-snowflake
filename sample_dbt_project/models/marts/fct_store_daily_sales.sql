with orders as (
    select * from {{ ref('stg_orders') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
store_daily_sales as (
    select
        o.store_id,
        cast(o.ordered_at as date) as ordered_date,
        p.product_type,
        count(distinct o.order_id) as order_count,
        count(oi.order_item_id) as item_count,
        sum(coalesce(p.price, 0)) as gross_sales
    from orders o
    left join order_items oi on o.order_id = oi.order_id
    left join products p on oi.sku = p.sku
    group by o.store_id, cast(o.ordered_at as date), p.product_type
)

select * from store_daily_sales
