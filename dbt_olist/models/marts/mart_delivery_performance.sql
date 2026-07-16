with orders as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    date_trunc('month', order_purchase_at) as delivery_month,
    count(distinct order_id) as total_delivered_orders,
    avg(delivery_delay_days) as avg_delivery_days,
    sum(case when is_late_delivery then 1 else 0 end) as late_deliveries_count,
    cast(sum(case when is_late_delivery then 1 else 0 end) as double) / count(distinct order_id) as late_delivery_rate
from orders
where order_status = 'delivered'
group by 1
order by 1 desc
