{{ config(materialized='view') }}

SELECT 
    account_id,
    symbol,
    shares,
    avg_cost,
    cost_basis,
    sector,
    industry,
    updated_at,
    created_at,
    
    -- Calculated fields
    CASE 
        WHEN shares > 0 AND avg_cost > 0 
        THEN shares * avg_cost 
        ELSE 0 
    END as total_cost_basis,
    
    -- Data quality flags
    CASE 
        WHEN shares > 0 AND avg_cost > 0 
        THEN true 
        ELSE false 
    END as is_valid_holding,
    
    CASE 
        WHEN shares * avg_cost > 1000000 
        THEN true 
        ELSE false 
    END as is_large_position,
    
    -- Position size categorization
    CASE 
        WHEN shares * avg_cost < 1000 THEN 'Small'
        WHEN shares * avg_cost < 10000 THEN 'Medium' 
        WHEN shares * avg_cost < 100000 THEN 'Large'
        ELSE 'Very Large'
    END as position_size_category

FROM {{ source('raw', 'raw_portfolio_holdings') }}
WHERE shares > 0
