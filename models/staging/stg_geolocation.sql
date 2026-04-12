-- 1. Import CTE: Reference the raw geolocation table defined in sources.yml
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_geolocation') }}
),

-- 2. Logical CTE: Perform type casting, fix leading zeros, and rename columns
renamed_and_casted AS (
    SELECT
        -- Convert bigint to string and pad with leading zeros to ensure a 5-digit format
        LPAD(CAST(geolocation_zip_code_prefix AS VARCHAR), 5, '0') AS zip_code_prefix, 

        -- Rename spatial columns for standardized readability
        CAST(geolocation_lat AS NUMERIC) AS latitude, 
        CAST(geolocation_lng AS NUMERIC) AS longitude,

        -- Keep geographical names as text
        CAST(geolocation_city AS VARCHAR) AS city,
        CAST(geolocation_state AS VARCHAR) AS state

    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted