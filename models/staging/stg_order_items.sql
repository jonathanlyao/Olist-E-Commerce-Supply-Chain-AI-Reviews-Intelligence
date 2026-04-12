-- 1. Import CTE: Reference the raw table defined in sources.yml
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_order_items')}}
), 

-- 2. Logical CTE: Perform type casting and column renaming
renamed_and_casted AS (
    SELECT 
        -- Identifiers
        CAST(order_id AS VARCHAR) AS order_id, 
        CAST(order_item_id AS INTEGER) AS item_sequence,
        CAST(product_id AS VARCHAR) AS product_id,
        CAST(seller_id AS VARCHAR) AS seller_id,

        -- Date casting
        CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_at, 

        -- Financial columns cast to numeric for precise calculation
        CAST(price AS NUMERIC) AS price,
        CAST(freight_value AS NUMERIC) AS freight_value

    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted