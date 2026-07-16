with sellers as (
    select * from {{ ref('int_seller_performance') }}
)

select
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    total_revenue,
    avg_review_score,
    avg_delivery_days,
    late_deliveries,
    
    -- Ranking based on revenue
    dense_rank() over (order by total_revenue desc) as revenue_rank,
    
    -- Ranking based on review score (min 10 orders to be considered)
    case 
        when total_orders >= 10 then dense_rank() over (partition by case when total_orders >= 10 then 1 else 0 end order by avg_review_score desc)
        else null
    end as review_rank
    
from sellers
