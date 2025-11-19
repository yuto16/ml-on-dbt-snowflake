
with source as (
    select * from {{ source('raw_jaffle_shop', 'orders') }}
)

select
    id as order_id,
    customer as customer_id,
    ordered_at,
    store_id,
    subtotal,
    tax_paid,
    order_total
from source
