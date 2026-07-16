with source as (
    select * from {{ source('raw', 'olist_order_reviews_dataset') }}
),

renamed as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        cast(review_creation_date as timestamp) as review_creation_at,
        cast(review_answer_timestamp as timestamp) as review_answered_at
    from source
)

select * from renamed
