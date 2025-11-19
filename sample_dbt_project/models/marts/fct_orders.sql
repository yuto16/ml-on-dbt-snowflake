
with orders as (
    select * from {{ ref('stg_orders') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
)
select
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.store_id,
    o.subtotal,
    o.tax_paid,
    o.order_total,
    oi.order_item_id,
    oi.sku,
    p.product_name,
    p.product_type,
    p.price
from orders o
left join order_items oi on o.order_id = oi.order_id
left join products p on oi.sku = p.sku
