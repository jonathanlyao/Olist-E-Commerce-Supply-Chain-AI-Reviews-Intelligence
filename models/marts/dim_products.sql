-- 1. Import CTEs: Reference our clean Staging models using the ref macro
WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
), 

translation AS (
    SELECT * FROM {{ ref('stg_product_category_name_translation') }}
), 

joined AS (
    SELECT 
        p.product_id, 
        p.category_name AS category_name_pt, 

        -- Use COALESCE to handle products that might not have a translation
        COALESCE(t.category_name_en, 'Unknown') AS category_name_en, 

        p.name_length, 
        p.description_length,
        p.photos_qty,
        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm

    FROM products AS p
    LEFT JOIN translation AS t
        ON p.category_name = t.category_name_pt
)

-- 3. Final Select 
SELECT * FROM joined