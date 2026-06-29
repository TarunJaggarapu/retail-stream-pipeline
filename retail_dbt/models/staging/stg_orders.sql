with source as (
    select * from {{ source('retail_analytics', 'orders') }}
)

select
    order_id,
    user_id,
    user_name,
    user_email,
    user_city,
    cast(order_date as timestamp) as order_date,
    total_amount_usd,
    total_amount_inr,
    currency_rate_usd_inr,
    cast(ingested_at as timestamp) as ingested_at,
    cast(processed_at as timestamp) as processed_at
from source