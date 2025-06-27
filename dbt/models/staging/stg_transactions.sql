{{ config(materialized='view') }}

SELECT 
    account_id,
    transaction_date,
    symbol,
    action,
    shares,
    price,
    fees,
    total_amount,
    notes,
    created_at,
    
    -- Calculated fields
    CASE 
        WHEN action = 'BUY' THEN shares * price + COALESCE(fees, 0)
        WHEN action = 'SELL' THEN shares * price - COALESCE(fees, 0)
        ELSE 0 
    END as net_amount,
    
    CASE 
        WHEN action = 'BUY' THEN shares
        WHEN action = 'SELL' THEN -shares
        ELSE 0 
    END as net_shares,
    
    -- Date categorization
    EXTRACT(YEAR FROM transaction_date) as transaction_year,
    EXTRACT(MONTH FROM transaction_date) as transaction_month,
    EXTRACT(QUARTER FROM transaction_date) as transaction_quarter,
    
    -- Data quality flags
    CASE 
        WHEN action IN ('BUY', 'SELL', 'DIVIDEND', 'SPLIT') 
        THEN true 
        ELSE false 
    END as is_valid_action,
    
    CASE 
        WHEN transaction_date <= CURRENT_DATE 
        THEN true 
        ELSE false 
    END as is_valid_date,
    
    CASE 
        WHEN shares > 0 AND price > 0 
        THEN true 
        ELSE false 
    END as is_valid_amounts

FROM {{ source('raw', 'raw_transactions') }}
WHERE 
    transaction_date >= '{{ var('min_transaction_date') }}'
    AND action IN ('BUY', 'SELL', 'DIVIDEND', 'SPLIT')
