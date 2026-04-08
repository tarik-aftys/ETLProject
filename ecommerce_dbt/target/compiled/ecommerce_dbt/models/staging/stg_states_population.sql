WITH source AS (
    SELECT * FROM "ecommerce_dw"."raw"."states_population"
),

renamed AS (
    SELECT
        state_code,
        state_name,
        -- On s'assure que la population est bien traitée comme un entier (Integer)
        CAST(population AS BIGINT) AS population
    FROM source
)

SELECT * FROM renamed