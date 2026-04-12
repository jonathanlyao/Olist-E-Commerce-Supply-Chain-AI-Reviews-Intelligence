-- 1. Import CTE: Reference the raw translation table
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_product_category_name_translation') }}
),

-- 2. Logical CTE: Perform type casting and rename columns for clarity
renamed_and_casted AS (
    SELECT
        -- Suffix added to explicitly denote language
        CAST(product_category_name AS VARCHAR) AS category_name_pt,
        CAST(product_category_name_english AS VARCHAR) AS category_name_en

    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted