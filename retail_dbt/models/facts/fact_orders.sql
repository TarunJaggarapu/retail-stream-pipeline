with orders as (
    select * from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(*) as total_items,
        sum(quantity) as total_quantity,
        sum(total_price_usd) as calculated_total_usd
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.user_id,
    o.user_name,
    o.user_city,
    o.order_date,
    o.total_amount_usd,
    o.total_amount_inr,
    o.currency_rate_usd_inr,
    i.total_items,
    i.total_quantity,
    i.calculated_total_usd,
    o.ingested_at,
    o.processed_at
from orders o
left join items i on o.order_id = i.order_id