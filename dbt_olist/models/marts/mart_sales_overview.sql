with orders as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    date_trunc('month', order_purchase_at) as sales_month,
    count(distinct order_id) as total_orders,
    sum(total_order_value) as total_revenue,
    avg(total_order_value) as average_order_value,
    count(distinct customer_id) as unique_customers,
    avg(avg_review_score) as avg_monthly_review_score
from orders
where order_status = 'delivered'
group by 1
order by 1 desc
