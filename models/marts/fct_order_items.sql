{{ config(materialized='table') }}

{# 1. Import CTE: Bring in our clean staging tables #}
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
), 

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
), 

{# 2. Construct the fact table via JOIN #}
final AS (
    SELECT
        -- Primary Key (The exact Grain: Order Line Item)
        i.order_id,
        i.item_sequence, 

        -- Foreign Key (Connectors to our Dimension Tables)
        o.customer_id,
        i.product_id,
        i.seller_id,

        -- Core Order Attributes 
        o.order_status,
        o.order_purchase_at, 
        o.order_delivered_at, 

        -- Facts / Metrics (The numbers BI tools will SUM and AVERAGE)
        i.price AS item_price, 
        i.freight_value AS item_freight, 

        -- Calculated Metric
        (i.price + i.freight_value) AS total_item_value

    FROM order_items AS i 
    LEFT JOIN orders AS o
        ON i.order_id = o.order_id
)

{# 3. Final Select #}
SELECT * FROM final