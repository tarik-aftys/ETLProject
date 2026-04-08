WITH source AS (
    SELECT * FROM {{ source('raw', 'order_items_dataset') }}
),

renamed AS (
    SELECT
        order_id,
        order_item_id, 
        product_id,
        seller_id,
        CAST(price AS NUMERIC(10,2)) AS price_brl,
        CAST(freight_value AS NUMERIC(10,2)) AS freight_brl
    FROM source
)

SELECT * FROM renamed