WITH source AS (
    SELECT * FROM {{ source('raw', 'exchange_rates') }}
),

renamed AS (
    SELECT
        CAST(date AS DATE) AS rate_date,
        CAST(usd_rate AS NUMERIC(10,4)) AS usd_rate
    FROM source
)

SELECT * FROM renamed