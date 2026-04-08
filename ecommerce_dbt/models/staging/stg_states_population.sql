WITH source AS (
    SELECT * FROM {{ source('raw', 'states_population') }}
),

renamed AS (
    SELECT
        state_code,
        state_name,
        CAST(population AS BIGINT) AS population
    FROM source
)

SELECT * FROM renamed