-- 1. Import CTE: Reference the raw table defined in sources.yml
WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_order_reviews')}}
), 

-- 2. Logical CTE: Perform type casting and column renaming
renamed_and_casted AS (
    SELECT 
        -- Identifiers
        CAST(review_id AS VARCHAR) AS review_id, 
        CAST(order_id AS INTEGER) AS order_id,

        -- Review metrics
        CAST(review_score AS INTEGER) AS review_score,
        

        -- Review text fields
        CAST(review_comment_title AS VARCHAR) AS review_comment_title,
        CAST(review_comment_message AS VARCHAR) AS review_comment_message,

        -- Timestamp casting and renaming
        CAST(review_creation_date AS TIMESTAMP) AS review_created_at,
        CAST(review_answer_timestamp AS TIMESTAMP) AS review_answered_at

    FROM source
)

-- 3. Final Select: Expose the transformed data
SELECT * FROM renamed_and_casted