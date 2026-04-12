-- 1. Import CTE: Reference the raw table defined in sources.yml
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_orders')}}
), 

-- 2. Logical CTE: Perform type casting and column renaming
renamed_and_casted AS (
    SELECT 
        order_id, 
        customer_id, 
        order_status, 

        -- Cast text to timestamp and standardize naming convention with '_at' suffix
        CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_at, 
        CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
        CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_shipped_at,
        CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_at,
        CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_at

    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted