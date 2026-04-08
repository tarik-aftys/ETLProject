
  create view "ecommerce_dw"."core"."stg_order_items__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "ecommerce_dw"."raw"."order_items_dataset"
),

renamed AS (
    SELECT
        order_id,
        order_item_id, -- L'identifiant de la ligne de commande
        product_id,
        seller_id,
        CAST(price AS NUMERIC(10,2)) AS price_brl,
        CAST(freight_value AS NUMERIC(10,2)) AS freight_brl
    FROM source
)

SELECT * FROM renamed
  );