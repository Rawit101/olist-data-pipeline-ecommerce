with source as (
    select * from {{ source('raw', 'olist_products_dataset') }}
),

translation as (
    select * from {{ source('raw', 'product_category_name_translation') }}
),

joined as (
    select
        s.product_id,
        coalesce(t.product_category_name_english, s.product_category_name) as product_category_name,
        s.product_name_lenght as product_name_length,
        s.product_description_lenght as product_description_length,
        s.product_photos_qty,
        s.product_weight_g,
        s.product_length_cm,
        s.product_height_cm,
        s.product_width_cm
    from source s
    left join translation t on s.product_category_name = t.product_category_name
)

select * from joined
