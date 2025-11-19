
with source as (
    select * from {{ source('raw_jaffle_shop', 'customers') }}
)

select
    id as customer_id,
    name as customer_name
from source
