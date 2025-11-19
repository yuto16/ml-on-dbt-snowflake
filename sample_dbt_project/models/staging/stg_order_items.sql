with source as (
    select * from {{ source('raw_jaffle_shop', 'order_items') }}
)

select
    id as order_item_id,
    order_id,
    sku
from source
