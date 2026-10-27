with orders as (
    select
        customer_id,
        count(*) as order_count,
        sum(case when status = 'completed' then amount else 0 end) as lifetime_value
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    coalesce(o.order_count, 0) as order_count,
    coalesce(o.lifetime_value, 0) as lifetime_value
from {{ ref('stg_customers') }} c
left join orders o using (customer_id)
