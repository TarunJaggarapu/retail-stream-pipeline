with orders as (
    select * from {{ ref('stg_orders') }}
)

select distinct
    user_id,
    user_name,
    user_email,
    user_city
from orders