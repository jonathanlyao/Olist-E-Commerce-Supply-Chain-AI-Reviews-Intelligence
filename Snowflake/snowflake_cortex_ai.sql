WITH raw_reviews AS (
    -- Extract base reviews from the raw staging layer
    SELECT * FROM {{ source('olist_raw', 'raw_order_reviews') }}
    WHERE review_comment_message IS NOT NULL
      
      -- Defensive Programming 1: Filter out meaningless short strings (e.g., 'ok', 'a', 'N/A')
      AND LENGTH(TRIM(review_comment_message)) > 3
      
      -- Defensive Programming 2: Explicitly exclude common invalid placeholders (case-insensitive)
      AND UPPER(TRIM(review_comment_message)) NOT IN ('N/A', 'NA', 'NONE', 'NULL')
)

ai_analysis AS (
    SELECT 
        review_id,
        review_comment_message,
        -- Generate raw LLM response
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3-8b', 
            CONCAT('Analyze the sentiment of this Portuguese e-commerce review. Output ONLY a single number from 1 to 10 (1 is very negative, 10 is very positive): ', review_comment_message)
        ) AS ai_sentiment_raw
    FROM raw_reviews
)