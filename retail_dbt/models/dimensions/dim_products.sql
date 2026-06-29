with items as (
    select * from {{ ref('stg_order_items') }}
)

select distinct
    product_id,
    product_title,
    category
from items