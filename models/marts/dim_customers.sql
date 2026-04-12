{# 1. Import CTE: Reference the clean staging customer data #}
WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

{# 2. Final Select: Expose attributes for BI slicing and dicing #}
final AS (
    SELECT 
        customer_id,
        customer_unique_id,
        zip_code_prefix,
        city,
        state
    FROM customers
)

SELECT * FROM final