with customers as (
    select * from {{ ref('stg_customers') }}
),

orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

customer_metrics as (
    select
        c.customer_unique_id,
        count(distinct o.order_id) as total_orders,
        sum(o.total_order_value) as total_spent,
        max(o.order_purchase_at) as last_purchase_at,
        
        -- Current date for recency calculation (mocked as the max date in dataset + 1 day)
        (select max(order_purchase_at) + interval 1 day from orders_enriched) as current_date_ref
    from customers c
    join orders_enriched o on c.customer_id = o.customer_id
    where o.order_status = 'delivered'
    group by 1
),

rfm as (
    select
        customer_unique_id,
        date_diff('day', last_purchase_at, current_date_ref) as recency_days,
        total_orders as frequency,
        total_spent as monetary
    from customer_metrics
)

select 
    r.*,
    c.customer_city,
    c.customer_state
from rfm r
join customers c on r.customer_unique_id = c.customer_unique_id
-- We might have duplicates if a unique customer has multiple customer_ids with different cities. 
-- For simplicity, let's just group by to get unique rows or window function.
qualify row_number() over (partition by r.customer_unique_id order by c.customer_id desc) = 1
