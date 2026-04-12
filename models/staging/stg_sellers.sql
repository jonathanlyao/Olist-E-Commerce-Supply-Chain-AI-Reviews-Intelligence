-- 1. Import CTE: Reference the raw sellers table
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_sellers') }}
),

-- 2. Logical CTE: Perform type casting, fix leading zeros, and rename columns
renamed_and_casted AS (
    SELECT
        -- Identifier
        CAST(seller_id AS VARCHAR) AS seller_id,
        
        -- Convert bigint to string and pad with leading zeros to ensure a 5-digit format
        LPAD(CAST(seller_zip_code_prefix AS VARCHAR), 5, '0') AS zip_code_prefix,
        
        -- Rename geographical columns to remove redundant 'seller_' prefix
        CAST(seller_city AS VARCHAR) AS city,
        CAST(seller_state AS VARCHAR) AS state

    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted