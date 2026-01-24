WITH sales_products AS (
    SELECT DISTINCT 
        product_id,
        product_name,
        unit_price as current_price
    FROM {{ ref('stg_sales') }}
)

SELECT
    product_id,
    product_name,
    current_price
FROM sales_products
