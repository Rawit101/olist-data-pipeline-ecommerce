with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    c.customer_state,
    c.customer_city,
    count(distinct o.order_id) as total_orders,
    sum(o.total_order_value) as total_revenue,
    avg(o.delivery_delay_days) as avg_delivery_days,
    avg(o.avg_review_score) as avg_review_score
from orders o
join customers c on o.customer_id = c.customer_id
where o.order_status = 'delivered'
group by 1, 2
order by 3 desc
