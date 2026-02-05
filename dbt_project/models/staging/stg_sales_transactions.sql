{{
  config(
    materialized='view',
    tags=['staging', 'sales']
  )
}}

/*
Staging model for sales transactions
Purpose: Clean and standardize raw sales data to universal schema
*/

WITH source AS (
  SELECT * FROM {{ source('raw_erp', 'sales_raw') }}
),

cleaned AS (
  SELECT
    -- Transaction identifiers
    CAST(transaction_id AS STRING) AS transaction_id,
    
    -- Date handling
    PARSE_DATE('%Y-%m-%d', transaction_date) AS transaction_date,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', transaction_date)) AS transaction_year,
    EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', transaction_date)) AS transaction_month,
    FORMAT_DATE('%B', PARSE_DATE('%Y-%m-%d', transaction_date)) AS transaction_month_name,
    EXTRACT(QUARTER FROM PARSE_DATE('%Y-%m-%d', transaction_date)) AS transaction_quarter,
    
    -- Customer information
    CAST(customer_name AS STRING) AS customer_id,  -- Using name as ID for now
    TRIM(UPPER(customer_name)) AS customer_name,
    
    -- Product information
    TRIM(product_name) AS product_name,
    CAST(product_name AS STRING) AS product_id,  -- Using name as ID for now
    
    -- Quantities and pricing
    CAST(quantity AS NUMERIC) AS quantity,
    CAST(unit_price AS NUMERIC) AS unit_price,
    CAST(cost_price AS NUMERIC) AS cost_price,
    CAST(total_amount AS NUMERIC) AS total_amount,
    
    -- Calculated fields: Margins
    CAST(unit_price AS NUMERIC) - COALESCE(CAST(cost_price AS NUMERIC), 0) AS unit_margin,
    
    CASE 
      WHEN CAST(unit_price AS NUMERIC) > 0 
      THEN (CAST(unit_price AS NUMERIC) - COALESCE(CAST(cost_price AS NUMERIC), 0)) / CAST(unit_price AS NUMERIC)
      ELSE 0
    END AS margin_percentage,
    
    -- Payment information
    TRIM(UPPER(COALESCE(payment_status, 'UNPAID'))) AS payment_status,
    
    SAFE.PARSE_DATE('%Y-%m-%d', payment_due_date) AS payment_due_date,
    
    -- Calculate days overdue (if unpaid)
    CASE 
      WHEN TRIM(UPPER(COALESCE(payment_status, 'UNPAID'))) = 'PAID' THEN 0
      WHEN SAFE.PARSE_DATE('%Y-%m-%d', payment_due_date) IS NULL THEN NULL
      ELSE DATE_DIFF(CURRENT_DATE(), SAFE.PARSE_DATE('%Y-%m-%d', payment_due_date), DAY)
    END AS days_overdue,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS loaded_at
    
  FROM source
  
  -- Data quality filters
  WHERE transaction_date IS NOT NULL
    AND quantity > 0
    AND total_amount > 0
)

SELECT * FROM cleaned
