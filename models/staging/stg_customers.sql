-- 1. Import CTE: Reference the raw customers table defined in sources.yml
WITH source AS (
    SELECT * FROM {{source('olist_raw', 'raw_customers')}}
), 

-- 2. Logical CTE: Perform type casting, fix leading zeros, and rename column names
renamed_and_casted AS (
    SELECT
        -- Identifiers
        CAST(customer_id AS VARCHAR) AS customer_id, 
        CAST(customer_unique_id AS VARCHAR) AS customer_unique_id, 

        -- Convert bigint to string and pad with leading zeros to ensure a 5-digit zipcode
        LPAD(CAST(customer_zip_code_prefix AS VARCHAR), 5, '0') AS zip_code_prefix, 

        -- Rename geographical columns to remove redundant 'customer_' prefix
        CAST(customer_city AS VARCHAR) AS city, 
        CAST(customer_state AS VARCHAR) AS state
    
    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted