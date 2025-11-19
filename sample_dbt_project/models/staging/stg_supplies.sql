with source as (
    select * from {{ source('raw_jaffle_shop', 'supplies') }}
)

select
    id as supply_id,
    name as supply_name,
    cost,
    perishable,
    sku
from source
