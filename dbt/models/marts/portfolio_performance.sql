{{ config(materialized='table') }}

WITH current_positions AS (
    SELECT 
        h.account_id,
        h.symbol,
        h.shares,
        h.avg_cost,
        h.cost_basis,
        h.sector,
        h.industry,
        h.position_size_category,
        h.updated_at
    FROM {{ ref('stg_portfolio_holdings') }} h
    WHERE h.is_valid_holding = true
),

latest_market_prices AS (
    SELECT 
        symbol,
        price as current_price,
        price_change_pct as daily_change_pct,
        volume,
        timestamp as price_timestamp
    FROM {{ ref('stg_market_data') }} 
    WHERE rn = 1 AND is_fresh = true
),

position_performance AS (
    SELECT 
        p.account_id,
        p.symbol,
        p.shares,
        p.avg_cost,
        p.cost_basis,
        p.sector,
        p.industry,
        p.position_size_category,
        
        -- Market data
        COALESCE(m.current_price, 0) as current_price,
        COALESCE(m.daily_change_pct, 0) as daily_change_pct,
        m.volume,
        m.price_timestamp,
        
        -- Performance calculations
        COALESCE(p.shares * m.current_price, 0) as market_value,
        COALESCE(p.shares * m.current_price - p.cost_basis, 0) as unrealized_pnl,
        
        CASE 
            WHEN p.cost_basis > 0 THEN 
                COALESCE((p.shares * m.current_price - p.cost_basis) / p.cost_basis * 100, 0)
            ELSE 0 
        END as return_percentage,
        
        CASE 
            WHEN p.shares * m.current_price > 0 THEN 
                COALESCE(p.shares * m.current_price - p.cost_basis, 0) / (p.shares * m.current_price)
            ELSE 0 
        END as profit_margin,
        
        p.updated_at
    FROM current_positions p
    LEFT JOIN latest_market_prices m ON p.symbol = m.symbol
),

portfolio_totals AS (
    SELECT 
        SUM(market_value) as total_portfolio_value,
        SUM(cost_basis) as total_cost_basis,
        SUM(unrealized_pnl) as total_unrealized_pnl
    FROM position_performance
),

final_portfolio AS (
    SELECT 
        pp.*,
        pt.total_portfolio_value,
        pt.total_cost_basis,
        pt.total_unrealized_pnl,
        
        -- Portfolio allocation
        CASE 
            WHEN pt.total_portfolio_value > 0 THEN 
                pp.market_value / pt.total_portfolio_value * 100 
            ELSE 0 
        END as allocation_percentage,
        
        -- Risk metrics (simplified)
        CASE 
            WHEN pp.market_value / pt.total_portfolio_value > 0.20 THEN 'High Concentration'
            WHEN pp.market_value / pt.total_portfolio_value > 0.10 THEN 'Medium Concentration'
            ELSE 'Diversified'
        END as concentration_risk,
        
        -- Performance categorization
        CASE 
            WHEN pp.return_percentage > 20 THEN 'Strong Performer'
            WHEN pp.return_percentage > 5 THEN 'Good Performer'
            WHEN pp.return_percentage > -5 THEN 'Neutral'
            WHEN pp.return_percentage > -20 THEN 'Underperformer'
            ELSE 'Poor Performer'
        END as performance_category,
        
        CURRENT_TIMESTAMP as analysis_timestamp
        
    FROM position_performance pp
    CROSS JOIN portfolio_totals pt
)

SELECT * FROM final_portfolio
ORDER BY market_value DESC
