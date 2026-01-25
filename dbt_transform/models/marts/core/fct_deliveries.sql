WITH deliveries AS (
    SELECT * FROM {{ ref('stg_inventory') }}
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    d.delivery_id,
    d.delivery_date,
    d.store_id,
    d.product_id,
    p.product_name, -- Enriched from Dimension
    d.quantity,
    d.status

FROM deliveries d
LEFT JOIN products p ON d.product_id = p.product_id