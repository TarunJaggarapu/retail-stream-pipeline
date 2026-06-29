with items as (
    select * from {{ ref('stg_order_items') }}
)

select
    category,
    sum(total_price_usd) as total_revenue_usd,
    sum(quantity) as total_units_sold,
    count(distinct order_id) as total_orders
from items
group by category
order by total_revenue_usd desc