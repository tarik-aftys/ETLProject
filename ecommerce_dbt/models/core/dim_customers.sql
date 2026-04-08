{{ config(
    materialized='table'
) }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

states AS (
    SELECT * FROM {{ ref('stg_states_population') }}
),

final_dim AS (
    SELECT
        c.customer_id,
        c.customer_unique_id,
        c.zip_code,
        c.city,
        c.state_code,
        s.state_name,
        s.population AS state_population
    FROM customers c
    LEFT JOIN states s
        ON c.state_code = s.state_code
)

SELECT * FROM final_dim