select
    id as order_id,
    user_id as customer_id,
    cast(order_date as date) as order_date,
    status,
    amount_cents / 100.0 as amount
from {{ ref('raw_orders') }}
