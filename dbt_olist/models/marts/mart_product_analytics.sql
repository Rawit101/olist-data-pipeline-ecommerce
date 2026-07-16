with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

product_sales as (
    select
        p.product_category_name,
        oi.product_id,
        count(distinct oi.order_id) as orders_count,
        sum(oi.price) as total_revenue,
        avg(o.avg_review_score) as avg_product_review_score
    from order_items oi
    join products p on oi.product_id = p.product_id
    join orders o on oi.order_id = o.order_id
    where o.order_status = 'delivered'
    group by 1, 2
)

select
    product_category_name,
    count(distinct product_id) as unique_products_sold,
    sum(orders_count) as total_category_orders,
    sum(total_revenue) as total_category_revenue,
    avg(avg_product_review_score) as avg_category_review_score
from product_sales
group by 1
order by total_category_revenue desc
