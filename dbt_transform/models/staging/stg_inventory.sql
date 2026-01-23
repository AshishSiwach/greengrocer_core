WITH source_data AS (
    SELECT * FROM {{ source('greengrocer_raw', 'raw_inventory') }}
),

cleaned_data AS (
    SELECT
        -- 1. Deduplication logic (just to be safe)
        *,
        ROW_NUMBER() OVER(PARTITION BY delivery_id ORDER BY delivery_date) as row_num
    FROM source_data
)

SELECT
    -- 2. Select and Rename Columns
    delivery_id,
    
    -- 3. Fix Data Types
    CAST(delivery_date AS DATE) as delivery_date,
    CAST(store_id AS VARCHAR) as store_id,
    CAST(product_id AS VARCHAR) as product_id,
    
    -- Rename 'quantity_delivered' to just 'quantity' for simplicity
    CAST(quantity_delivered AS INTEGER) as quantity,
    
    -- Keep the status (e.g., 'Delivered', 'Damaged')
    CAST(delivery_status AS VARCHAR) as status

FROM cleaned_data
WHERE row_num = 1 -- Filter out duplicates