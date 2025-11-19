with source as (
    select * from {{ source('raw_jaffle_shop', 'stores') }}
)

select
    id as store_id,
    name as store_name,
    opened_at,
    tax_rate
from source
