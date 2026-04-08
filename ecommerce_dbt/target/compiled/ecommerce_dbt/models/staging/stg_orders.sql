WITH source AS (
    SELECT * FROM "ecommerce_dw"."raw"."orders_dataset"
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        order_status AS status,
        -- Concept Ingénieur : Toujours s'assurer du bon typage des dates dès le Staging
        CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_ts,
        CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_ts,
        CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts
    FROM source
)

SELECT * FROM renamed