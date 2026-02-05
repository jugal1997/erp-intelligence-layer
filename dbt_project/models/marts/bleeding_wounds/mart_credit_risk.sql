{{
  config(
    materialized='table',
    tags=['mart', 'alert', 'credit_risk']
  )
}}

/*
Mart: Credit Risk Analysis
Purpose: Identify customers with overdue payments
Business Impact: Cash flow problems, bad debt risk
*/

WITH sales AS (
  SELECT * FROM {{ ref('int_sales_with_metrics') }}
),

-- Focus only on unpaid transactions
unpaid_transactions AS (
  SELECT
    customer_id,
    customer_name,
    customer_lifetime_value,
    COUNT(DISTINCT transaction_id) AS overdue_invoice_count,
    SUM(total_amount) AS total_overdue_amount,
    MAX(days_overdue) AS max_days_overdue,
    AVG(days_overdue) AS avg_days_overdue,
    MIN(transaction_date) AS oldest_unpaid_date,
    
    -- List of overdue invoice IDs (for reference)
    STRING_AGG(transaction_id, ', ' ORDER BY transaction_date) AS overdue_invoice_ids
    
  FROM sales
  WHERE payment_status != 'PAID'
    AND days_overdue >= {{ var('credit_risk_days') }}
  GROUP BY customer_id, customer_name, customer_lifetime_value
),

credit_risk_analysis AS (
  SELECT
    customer_id,
    customer_name,
    overdue_invoice_count,
    total_overdue_amount,
    max_days_overdue,
    ROUND(avg_days_overdue, 0) AS avg_days_overdue,
    oldest_unpaid_date,
    customer_lifetime_value,
    overdue_invoice_ids,
    
    -- Calculate payment score (0-100, lower is worse)
    CASE
      WHEN max_days_overdue > 90 THEN 0
      WHEN max_days_overdue > 60 THEN 25
      WHEN max_days_overdue > 30 THEN 50
      WHEN max_days_overdue > 15 THEN 75
      ELSE 100
    END AS payment_score,
    
    -- Risk as percentage of lifetime value
    ROUND((total_overdue_amount / NULLIF(customer_lifetime_value, 0)) * 100, 2) AS risk_percentage_of_ltv,
    
    -- Alert severity (multiple conditions)
    CASE
      -- Critical if either high amount + overdue OR very old debt
      WHEN total_overdue_amount > 100000 AND max_days_overdue > 60 THEN 'CRITICAL'
      WHEN max_days_overdue > 90 THEN 'CRITICAL'
      WHEN max_days_overdue > 60 THEN 'HIGH'
      WHEN max_days_overdue > 30 THEN 'MEDIUM'
      WHEN max_days_overdue > {{ var('credit_risk_days') }} THEN 'LOW'
      ELSE 'INFO'
    END AS alert_severity,
    
    -- Recommended action
    CASE
      WHEN max_days_overdue > 90 THEN 'STOP ALL CREDIT. Initiate legal recovery process.'
      WHEN max_days_overdue > 60 THEN 'Final notice. Stop further sales immediately.'
      WHEN max_days_overdue > 30 THEN 'Send stern payment reminder. Personal follow-up required.'
      ELSE 'Send polite payment reminder via WhatsApp'
    END AS recommended_action,
    
    -- Analysis metadata
    CURRENT_DATE() AS analysis_date,
    CURRENT_TIMESTAMP() AS created_at
    
  FROM unpaid_transactions
)

SELECT * FROM credit_risk_analysis
ORDER BY alert_severity DESC, total_overdue_amount DESC
