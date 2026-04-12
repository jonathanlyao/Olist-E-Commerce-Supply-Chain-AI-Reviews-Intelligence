-- 1. Import CTE: Reference the raw products table
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_products') }}
),

-- 2. Logical CTE: Perform type casting, rename columns, and fix spelling errors
renamed_and_casted AS (
    SELECT
        -- Identifiers and Categorical dimensions
        CAST(product_id AS VARCHAR) AS product_id,
        CAST(product_category_name AS VARCHAR) AS category_name,
        
        -- Text attributes (fixing 'lenght' spelling typo from raw data)
        CAST(product_name_lenght AS INTEGER) AS name_length,
        CAST(product_description_lenght AS INTEGER) AS description_length,
        CAST(product_photos_qty AS INTEGER) AS photos_qty,
        
        -- Physical dimensions cast to integers
        CAST(product_weight_g AS INTEGER) AS weight_g,
        CAST(product_length_cm AS INTEGER) AS length_cm,
        CAST(product_height_cm AS INTEGER) AS height_cm,
        CAST(product_width_cm AS INTEGER) AS width_cm

    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted