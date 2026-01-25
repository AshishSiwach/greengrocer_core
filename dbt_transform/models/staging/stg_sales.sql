WITH source_data AS (
    SELECT * FROM {{ source('greengrocer_raw', 'raw_sales') }}
),

cleaned_data AS (
    SELECT
        -- 1. Handle Duplicates: Generate a row number for each transaction_id
        *,
        ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY sale_date) as row_num
    FROM source_data
)

SELECT
    -- 2. Select only the "first" occurrence of each transaction
    transaction_id,
    
    -- 3. Fix Data Types (Casting String to Real Types)
    CAST(sale_date AS TIMESTAMP) as transaction_at,  -- <--- FIXED THIS
    CAST(product_id AS VARCHAR) as product_id,
    CAST(product_name AS VARCHAR) as product_name,
    CAST(quantity AS INTEGER) as quantity,
    CAST(unit_price AS DECIMAL(10,2)) as unit_price,
    CAST(total_amount AS DECIMAL(10,2)) as total_amount

FROM cleaned_data
WHERE row_num = 1 -- <--- This filters out the duplicates!