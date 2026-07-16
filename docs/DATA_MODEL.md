# Data Model

The pipeline transforms data through three main layers: Bronze (Raw), Silver (Staging/Intermediate), and Gold (Marts).

## 1. Bronze Layer (Raw)
Loaded directly from Kaggle CSVs into MinIO and then DuckDB.
- `raw.olist_orders_dataset`
- `raw.olist_order_items_dataset`
- `raw.olist_customers_dataset`
- `raw.olist_sellers_dataset`
- `raw.olist_products_dataset`
- `raw.olist_order_payments_dataset`
- `raw.olist_order_reviews_dataset`
- `raw.olist_geolocation_dataset`
- `raw.product_category_name_translation`

## 2. Silver Layer (Staging)
Data is cleaned, typed, and basic standardizations are applied.
- `stg_orders`: Timestamps cast, statuses standardized
- `stg_order_items`: Price/Freight casts
- `stg_customers`: Unique customer mapping
- `stg_sellers`: Seller location mapping
- `stg_products`: Portuguese category names translated to English
- `stg_payments`: Payment values cast
- `stg_reviews`: Review dates cast

## 3. Silver Layer (Intermediate)
Business logic and joins are applied before aggregations.
- `int_orders_enriched`: Joins orders with payments, reviews, and delivery delays.
- `int_seller_performance`: Base metrics per seller order.
- `int_customer_segmentation`: Base metrics for RFM per customer.

## 4. Gold Layer (Marts)
Aggregated tables ready for dashboards.
- `mart_sales_overview`: Monthly revenue, orders, AOV.
- `mart_delivery_performance`: SLA tracking, late delivery rates.
- `mart_product_analytics`: Category performance, top products.
- `mart_customer_rfm`: Recency, Frequency, Monetary scoring.
- `mart_seller_ranking`: Top sellers by revenue and rating.
- `mart_geo_analysis`: Sales and delivery metrics by state/city.
