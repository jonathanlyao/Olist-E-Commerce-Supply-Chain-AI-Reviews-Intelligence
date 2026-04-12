{# 1. Import CTE: Reference the clean staging seller data #}
WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),

{# 2. Final Select: Expose attributes for BI slicing and dicing #}
final AS (
    SELECT
        seller_id,
        zip_code_prefix,
        city,
        state
    FROM sellers
)

SELECT * FROM final