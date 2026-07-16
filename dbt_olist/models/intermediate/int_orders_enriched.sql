with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_payments as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        count(payment_sequential) as payment_installments_count
    from {{ ref('stg_payments') }}
    group by 1
),

order_reviews as (
    select
        order_id,
        avg(review_score) as avg_review_score
    from {{ ref('stg_reviews') }}
    group by 1
),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_at,
        o.order_approved_at,
        o.order_delivered_carrier_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,
        
        -- Delivery performance
        date_diff('day', o.order_estimated_delivery_at, o.order_delivered_customer_at) as delivery_delay_days,
        case 
            when o.order_delivered_customer_at > o.order_estimated_delivery_at then true 
            else false 
        end as is_late_delivery,
        
        -- Value
        coalesce(p.total_payment_value, 0) as total_order_value,
        
        -- Reviews
        r.avg_review_score
        
    from orders o
    left join order_payments p on o.order_id = p.order_id
    left join order_reviews r on o.order_id = r.order_id
)

select * from enriched
