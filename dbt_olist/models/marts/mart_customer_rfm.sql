with customer_rfm as (
    select * from {{ ref('int_customer_segmentation') }}
)

select
    customer_unique_id,
    customer_city,
    customer_state,
    recency_days,
    frequency,
    monetary,
    
    -- Simple RFM scoring (1-5 for each based on percentiles or fixed bins would be better, but keeping it simple here)
    case 
        when recency_days <= 30 then 5
        when recency_days <= 90 then 4
        when recency_days <= 180 then 3
        when recency_days <= 365 then 2
        else 1
    end as r_score,
    
    case 
        when frequency >= 5 then 5
        when frequency >= 3 then 4
        when frequency >= 2 then 3
        else 1
    end as f_score,
    
    case
        when monetary >= 1000 then 5
        when monetary >= 500 then 4
        when monetary >= 200 then 3
        when monetary >= 100 then 2
        else 1
    end as m_score

from customer_rfm
