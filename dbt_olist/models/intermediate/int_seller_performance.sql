with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

reviews as (
    select * from {{ ref('stg_reviews') }}
),

seller_orders as (
    select
        oi.seller_id,
        oi.order_id,
        oi.price,
        oi.freight_value,
        o.order_status,
        o.order_purchase_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,
        r.review_score
    from order_items oi
    join orders o on oi.order_id = o.order_id
    left join reviews r on o.order_id = r.order_id
),

seller_metrics as (
    select
        seller_id,
        count(distinct order_id) as total_orders,
        sum(price) as total_revenue,
        sum(freight_value) as total_freight,
        avg(review_score) as avg_review_score,
        
        -- delivery speed
        avg(date_diff('day', order_purchase_at, order_delivered_customer_at)) as avg_delivery_days,
        
        -- late deliveries
        sum(case when order_delivered_customer_at > order_estimated_delivery_at then 1 else 0 end) as late_deliveries
        
    from seller_orders
    where order_status = 'delivered'
    group by 1
)

select 
    sm.*,
    s.seller_city,
    s.seller_state
from seller_metrics sm
join sellers s on sm.seller_id = s.seller_id
