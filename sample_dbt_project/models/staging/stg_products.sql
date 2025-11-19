with source as (
    select * from {{ source('raw_jaffle_shop', 'products') }}
)

select
    sku,
    name as product_name,
    type as product_type,
    price,
    description
from source
