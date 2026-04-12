CREATE OR REPLACE STORAGE INTEGRATION s3_olist_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::071680046313:role/snowflake_olist_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://olist-data-lake-leeyao-oregon/');

DESCRIBE INTEGRATION s3_olist_int;

-- 1. Create the database named "OLIST_DB"
CREATE DATABASE IF NOT EXISTS OLIST_DB; 

-- 2. Tell the system how we would operate this database
USE DATABASE OLIST_DB; 

-- 3. Create a schema for raw data in this database
CREATE SCHEMA IF NOT EXISTS RAW_DATA; 

-- 4. Tell system that we will use this schema 
USE SCHEMA RAW_DATA; 


-- 1. Customers
CREATE OR REPLACE TABLE raw_customers (
    customer_id VARCHAR, 
    customer_unique_id VARCHAR,
    customer_zip_code_prefix VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR
    
); 

-- 2. Geolocation
CREATE OR REPLACE TABLE raw_geolocation (
    geolocation_zip_code_prefix VARCHAR,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR,
    geolocation_state VARCHAR
);

-- 3. Order items
CREATE OR REPLACE TABLE raw_order_items (
    order_id VARCHAR,
    order_item_id INTEGER,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT
);

-- 4. Order payments
CREATE OR REPLACE TABLE raw_order_payments (
    order_id VARCHAR,
    payment_sequential INTEGER,
    payment_type VARCHAR,
    payment_installments INTEGER,
    payment_value FLOAT
);

-- 5. Order reviews
CREATE OR REPLACE TABLE raw_order_reviews (
    review_id VARCHAR,
    order_id VARCHAR,
    review_score INTEGER,
    review_comment_title VARCHAR,
    review_comment_message VARCHAR,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- 6. Orders
CREATE OR REPLACE TABLE raw_orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- 7. Products
CREATE OR REPLACE TABLE raw_products (
    product_id VARCHAR,
    product_category_name VARCHAR,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT
);

-- 8. Sellers
CREATE OR REPLACE TABLE raw_sellers (
    seller_id VARCHAR,
    seller_zip_code_prefix VARCHAR,
    seller_city VARCHAR,
    seller_state VARCHAR
);

-- 9. Product category name translation
CREATE OR REPLACE TABLE raw_product_category_name_translation (
    product_category_name VARCHAR,
    product_category_name_english VARCHAR
);

------------------------------------------------------------

-- Make sure we are in the correct schemas
USE DATABASE OLIST_DB; 
USE SCHEMA RAW_DATA; 

-- 1. Create the file format called olist_csv_format
CREATE OR REPLACE FILE FORMAT olist_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null');

-- 2. Create a stage called olist_s3_stage
CREATE OR REPLACE STAGE olist_s3_stage
    URL = 's3://olist-data-lake-leeyao-oregon/'
    STORAGE_INTEGRATION = s3_olist_int
    FILE_FORMAT = olist_csv_format; 

-- 3. Testing stage: see if we can find the files imported from S3 to Snowflake
LIST @olist_s3_stage; 


----------------------------------------------------------------

-- COPY INTO the schema now 

-- Make sure we are in the correct schemas 
USE DATABASE OLIST_DB; 
USE SCHEMA RAW_DATA; 

-- 1. Load customers dataset 
COPY INTO raw_customers
FROM @olist_s3_stage/olist_customers_dataset.csv
ON_ERROR = 'CONTINUE'; 

-- 2. Load geolocation dataset
COPY INTO raw_geolocation 
FROM @olist_s3_stage/olist_geolocation_dataset.csv 
ON_ERROR = 'CONTINUE';

-- 3. Load order items dataset
COPY INTO raw_order_items 
FROM @olist_s3_stage/olist_order_items_dataset.csv 
ON_ERROR = 'CONTINUE';

-- 4. Load order payments dataset 
COPY INTO raw_order_payments 
FROM @olist_s3_stage/olist_order_payments_dataset.csv 
ON_ERROR = 'CONTINUE';

-- 5. Load order reviews dataset
COPY INTO raw_order_reviews 
FROM @olist_s3_stage/olist_order_reviews_dataset.csv 
ON_ERROR = 'CONTINUE';

-- 6. Load orders dataset 
COPY INTO raw_orders 
FROM @olist_s3_stage/olist_orders_dataset.csv 
ON_ERROR = 'CONTINUE';

-- 7. Load products dataset 
COPY INTO raw_products 
FROM @olist_s3_stage/olist_products_dataset.csv 
ON_ERROR = 'CONTINUE';

-- 8. Load sellers dataset 
COPY INTO raw_sellers 
FROM @olist_s3_stage/olist_sellers_dataset.csv 
ON_ERROR = 'CONTINUE';

-- 9. Load product category name translation dataset 
COPY INTO raw_product_category_name_translation 
FROM @olist_s3_stage/product_category_name_translation.csv 
ON_ERROR = 'CONTINUE';

--------------------------------------------------------
SELECT * FROM raw_orders LIMIT 10;

-- Force warehouse to suspend if idle for 1 minute
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60; 

-- For the orders' reviews, I'm going to use AI in snowflake to interpret them
-- First，I need to make sure if AI is available in snowflake. 
SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', 'Translate to English Olá, como vai?'); ’

SELECT COUNT(*) FROM olist_db.dbt_dev.stg_reviews_enriched; 

SELECT 
    r.review_id::VARCHAR AS review_id, 
    o.order_id::VARCHAR AS order_id, 
    r.sentiment_score 
FROM olist_db.dbt_dev.stg_reviews_enriched r
JOIN olist_db.raw_data.raw_order_reviews o 
  ON r.review_id::VARCHAR = o.review_id::VARCHAR
WHERE r.sentiment_score IS NOT NULL
LIMIT 500;