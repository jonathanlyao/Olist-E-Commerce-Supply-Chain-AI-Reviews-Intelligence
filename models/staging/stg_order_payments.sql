-- 1. Import CTE: Reference the raw table defined in sources.yml
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_order_payments')}}
), 

-- 2. Logical CTE: Perform type casting and column renaming
renamed_and_casted AS (
    SELECT 
        -- Identifiers
        CAST(order_id AS VARCHAR) AS order_id, 
        CAST(payment_sequential AS INTEGER) AS payment_sequence,

        -- Categorical dimensions
        CAST(payment_type AS VARCHAR) AS payment_type,
        CAST(payment_installments AS INTEGER) AS payment_installments,

        -- Financial metrics cast to NUMERIC for absolute precision
        CAST(payment_value AS NUMERIC) AS payment_amount

    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted