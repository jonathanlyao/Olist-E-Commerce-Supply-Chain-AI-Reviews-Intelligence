{{ config(materialized='table') }}

WITH base_reviews AS (
    -- Reference the previously cleaned staging table for base review data (contains order_id)
    SELECT * FROM {{ ref('stg_order_reviews') }}
),

ai_enriched_reviews AS (
    -- Reference the newly created AI enrichment view
    SELECT * FROM {{ ref('stg_reviews_enriched') }}
),

final_dimension AS (
    SELECT 
        -- Primary Key
        b.review_id,
        
        -- Foreign Key (Connects back to FCT_ORDER_ITEMS via ORDER_ID)
        b.order_id,
        
        -- Native attributes
        b.review_score AS original_star_rating,
        
        -- AI-Generated attributes
        a.sentiment_score AS ai_sentiment_score,
        a.review_comment_message
        
    FROM base_reviews AS b
    LEFT JOIN ai_enriched_reviews AS a
        ON b.review_id = a.review_id
)

SELECT * FROM final_dimension