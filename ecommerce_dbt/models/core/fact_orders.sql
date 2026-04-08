{{ config(
    materialized='table'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

exchange_rates AS (
    SELECT * FROM {{ ref('stg_exchange_rates') }}
),

order_totals AS (
    SELECT 
        order_id,
        COUNT(order_item_id) AS total_items,
        SUM(price_brl) AS total_price_brl,
        SUM(freight_brl) AS total_freight_brl
    FROM order_items
    GROUP BY 1
),

final_fact AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.purchase_ts AS order_date,
        o.status AS order_status,
        COALESCE(t.total_items, 0) AS total_items,
        COALESCE(t.total_price_brl, 0) AS revenue_brl,
        ROUND(COALESCE(t.total_price_brl, 0) * r.usd_rate, 2) AS revenue_usd,
        r.usd_rate AS current_exchange_rate
    FROM orders o
    LEFT JOIN order_totals t 
        ON o.order_id = t.order_id
        
    LEFT JOIN exchange_rates r 
        ON DATE_TRUNC('day', o.purchase_ts) = r.rate_date
)

SELECT * FROM final_fact