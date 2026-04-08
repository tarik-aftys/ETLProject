
  
    

  create  table "ecommerce_dw"."core"."dim_customers__dbt_tmp"
  
  
    as
  
  (
    

WITH customers AS (
    SELECT * FROM "ecommerce_dw"."core"."stg_customers"
),

states AS (
    SELECT * FROM "ecommerce_dw"."core"."stg_states_population"
),

final_dim AS (
    SELECT
        c.customer_id,
        c.customer_unique_id,
        c.zip_code,
        c.city,
        c.state_code,
        
        -- ⭐ LA MAGIE : L'enrichissement via Web Scraping !
        s.state_name,
        s.population AS state_population
        
    FROM customers c
    -- On joint nos données métiers Olist avec les données Wikipédia via le code de l'État (ex: "SP", "RJ")
    LEFT JOIN states s
        ON c.state_code = s.state_code
)

SELECT * FROM final_dim
  );
  