
  create view "ecommerce_dw"."core"."stg_customers__dbt_tmp"
    
    
  as (
    WITH source AS (
    -- La macro source() est magique : dbt génère le bon chemin (raw.customers_dataset)
    -- et trace la dépendance (Lignage de la donnée)
    SELECT * FROM "ecommerce_dw"."raw"."customers_dataset"
),

renamed AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix AS zip_code,
        customer_city AS city,
        customer_state AS state_code
    FROM source
)

SELECT * FROM renamed
  );