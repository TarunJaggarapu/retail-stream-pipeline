with fact as (
    select * from {{ ref('fact_orders') }}
)

select
    date(order_date) as order_day,
    count(distinct order_id) as total_orders,
    sum(total_amount_usd) as total_revenue_usd,
    sum(total_amount_inr) as total_revenue_inr,
    avg(total_amount_usd) as avg_order_value_usd
from fact
group by date(order_date)
order by order_day desc