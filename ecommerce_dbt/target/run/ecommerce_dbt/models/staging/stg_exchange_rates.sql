
  create view "ecommerce_dw"."core"."stg_exchange_rates__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "ecommerce_dw"."raw"."exchange_rates"
),

renamed AS (
    SELECT
        CAST(date AS DATE) AS rate_date,
        -- Précision financière (numeric/decimal est obligatoire pour l'argent)
        CAST(usd_rate AS NUMERIC(10,4)) AS usd_rate
    FROM source
)

SELECT * FROM renamed
  );