{{ config(materialized='table') }}

WITH raw_reviews AS (
    SELECT * FROM {{ source('olist_raw', 'raw_order_reviews') }} -- Import raw_review_data
    WHERE review_comment_message IS NOT NULL -- Filter for reviews that contain actual text comments

    AND LENGTH(TRIM(review_comment_message)) > 3 -- Defensive Strategy: 1. Filter out meaningless short strings such as 'ok', 'a', 'N/A'

    AND UPPER(TRIM(review_comment_message)) NOT IN ('N/A', 'NA', 'NONE', 'NULL') -- Defensive Strategy: 2. Explicitly exlcude common invalid placeholders (case-insensitive)
), 

ai_analysis AS (
    SELECT 
        review_id, 
        review_comment_message, 
        -- Invoke Snowflake Cortex LLM(llama3-8b) for sentiment analysis 
        -- Prompt strictly limits output to a single integer (1-10) for robust downstream aggregation
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3-8b', 
            CONCAT('Anayze the sentiment of this Portuguese e-commerce review. Output ONLY a single number from 1 to 10 (1 is very negative, 10 is very positive): ', review_comment_message)
        ) AS ai_sentiment_raw
    FROM raw_reviews
)

SELECT
    review_id, 
    review_comment_message,
    ai_sentiment_raw, -- Debugging column: Keep the raw output so we can see exactly what the LLM said
    TRY_TO_NUMBER(REGEXP_SUBSTR(ai_sentiment_raw, '\\d+')) AS sentiment_score -- Robust parsing: Use REGEXP_SUBSTR to extract the first continuous sequence of digits(\\d+). Then safely cast that extracted sequence to a NUMBER
FROM ai_analysis