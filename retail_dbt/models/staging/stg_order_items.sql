with source as (
    select * from {{ source('retail_analytics', 'order_items') }}
)

select
    order_id,
    product_id,
    product_title,
    category,
    quantity,
    unit_price_usd,
    total_price_usd
from source