with items as (
    select * from {{ ref('stg_order_items') }}
)

select
    product_id,
    product_title,
    category,
    sum(quantity) as total_quantity_sold,
    sum(total_price_usd) as total_revenue_usd,
    count(distinct order_id) as times_ordered
from items
group by product_id, product_title, category
order by total_revenue_usd desc