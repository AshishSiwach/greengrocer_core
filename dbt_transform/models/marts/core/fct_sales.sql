WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    s.transaction_id,
    s.sale_date,
    s.store_id,
    s.product_id,
    p.product_name,
    s.quantity,
    s.unit_price,
    s.total_amount

FROM sales s
LEFT JOIN products p ON s.product_id = p.product_id
